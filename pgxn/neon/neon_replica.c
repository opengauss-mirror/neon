/*
 * Committed WAL input for openGauss Neon read replicas.
 *
 * Safekeepers speak PostgreSQL's streaming protocol, not openGauss's HA
 * protocol. Feed that stream directly to the startup process's XLogReader.
 * Only one WAL page and one incoming message are buffered here; relation
 * storage remains on the pageserver. Native recovery owns replay, snapshots,
 * recovery conflicts and cache coherence.
 */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqformat.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/timestamp.h"

#include "neon.h"
#include "pagestore_client.h"
#include "wait_events.h"
#include "walproposer.h"

static THR_LOCAL PGconn *replica_conn;
static THR_LOCAL char *replica_message;
static THR_LOCAL int replica_message_len;
static THR_LOCAL int replica_message_pos;
static THR_LOCAL XLogRecPtr replica_stream_lsn;
static THR_LOCAL XLogRecPtr replica_page_lsn;
static THR_LOCAL char replica_page[XLOG_BLCKSZ];
static THR_LOCAL int replica_page_len;
static THR_LOCAL unsigned replica_donor;
static THR_LOCAL TimestampTz replica_last_reply;
static THR_LOCAL TimestampTz replica_last_message;
static THR_LOCAL bool replica_exit_registered;

static void
replica_disconnect(int code, Datum arg)
{
	if (replica_message)
		PQfreemem(replica_message);
	replica_message = NULL;
	replica_message_len = replica_message_pos = 0;
	if (replica_conn)
		PQfinish(replica_conn);
	replica_conn = NULL;
}

static void
replica_wait(int events)
{
	RedoInterruptCallBack();
	ResetLatch(&t_thrd.proc->procLatch);
	NeonWaitLatchOrSocket(&t_thrd.proc->procLatch,
		WL_LATCH_SET | WL_TIMEOUT | events | WL_EXIT_ON_PM_DEATH,
		replica_conn ? PQsocket(replica_conn) : PGINVALID_SOCKET,
		1000L, WAIT_EVENT_NEON_WAL_DL);
	RedoInterruptCallBack();
	CheckRecoveryConsistency();
}

/* Report replay progress even while idle, so safekeepers can protect GC. */
static bool
replica_reply(bool force)
{
	TimestampTz now = GetCurrentTimestamp();
	StringInfoData reply;
	XLogRecPtr replay;
	int rc;

	if (!force && !TimestampDifferenceExceeds(replica_last_reply, now, 1000))
		return true;
	if (PQflush(replica_conn) < 0)
		return false;
	replay = GetXLogReplayRecPtr(NULL);
	initStringInfo(&reply);
	pq_sendbyte(&reply, 'r');
	pq_sendint64(&reply, replay);
	pq_sendint64(&reply, replay);
	pq_sendint64(&reply, replay);
	pq_sendint64(&reply, now);
	pq_sendbyte(&reply, false);
	rc = PQputCopyData(replica_conn, reply.data, reply.len);
	pfree(reply.data);
	if (rc < 0)
		return false;
	if (rc > 0)
		replica_last_reply = now;
	return PQflush(replica_conn) >= 0;
}

/* Rotate sources on failure. All streams omit 'term' and contain committed WAL. */
static bool
replica_connect(XLogRecPtr start)
{
	char *list = pstrdup(GetWalAcceptorsList());
	char *addresses = list;
	char *hosts[MAX_SAFEKEEPERS];
	int nhosts = 0;
	char *saveptr = NULL;
	char *address;
	char *port;
	char *host;
	char options[256];
	const char *keys[] = {"host", "port", "user", "dbname", "replication",
		"application_name", "options", "password", "connect_timeout", NULL};
	const char *values[10];
	TimestampTz began = GetCurrentTimestamp();
	PGresult *res;
	char query[100];

	if (strncmp(addresses, "g#", 2) == 0)
	{
		addresses = strchr(addresses, ':');
		if (addresses)
			addresses++;
	}
	if (addresses)
		for (address = strtok_r(addresses, ",", &saveptr); address;
			 address = strtok_r(NULL, ",", &saveptr))
		{
			if (nhosts == MAX_SAFEKEEPERS)
				ereport(FATAL, (errmsg("too many Neon replica WAL sources")));
			hosts[nhosts++] = address;
		}
	if (nhosts == 0)
		ereport(FATAL, (errmsg("Neon replica requires neon.safekeepers")));
	host = hosts[replica_donor++ % nhosts];
	port = strrchr(host, ':');
	if (!port || port == host || !port[1])
		ereport(FATAL, (errmsg("invalid Neon replica WAL source address")));
	*port++ = '\0';
	if (host[0] == '[' && host[strlen(host) - 1] == ']')
	{
		host[strlen(host) - 1] = '\0';
		host++;
	}
	snprintf(options, sizeof(options), "-c timeline_id=%s tenant_id=%s",
		GetNeonTimelineId(), GetNeonTenantId());
	values[0] = host;
	values[1] = port;
	values[2] = "no_user";
	values[3] = "replication";
	values[4] = "true";
	values[5] = "neon_read_replica";
	values[6] = options;
	values[7] = neon_auth_token ? neon_auth_token : getenv("NEON_AUTH_TOKEN");
	values[8] = "5";
	values[9] = NULL;
	replica_conn = PQconnectStartParams(keys, values, 0);
	pfree(list);
	if (!replica_conn)
		ereport(FATAL, (errmsg("out of memory connecting Neon replica WAL reader")));
	for (;;)
	{
		PostgresPollingStatusType status = PQconnectPoll(replica_conn);
		if (status == PGRES_POLLING_OK)
			break;
		if (status == PGRES_POLLING_FAILED ||
			TimestampDifferenceExceeds(began, GetCurrentTimestamp(), 5000))
			return false;
		replica_wait(status == PGRES_POLLING_WRITING ? WL_SOCKET_WRITEABLE : WL_SOCKET_READABLE);
	}
	if (PQsetnonblocking(replica_conn, 1) != 0)
		return false;
	snprintf(query, sizeof(query), "START_REPLICATION PHYSICAL %X/%X", LSN_FORMAT_ARGS(start));
	if (!PQsendQuery(replica_conn, query))
		return false;
	for (;;)
	{
		int pending = PQflush(replica_conn);
		if (pending < 0 || !PQconsumeInput(replica_conn))
			return false;
		if (!pending && !PQisBusy(replica_conn))
			break;
		if (TimestampDifferenceExceeds(began, GetCurrentTimestamp(), 10000))
			return false;
		replica_wait(pending ? WL_SOCKET_WRITEABLE : WL_SOCKET_READABLE);
	}
	res = PQgetResult(replica_conn);
	bool ok = res && PQresultStatus(res) == PGRES_COPY_BOTH;
	if (res)
		PQclear(res);
	if (!ok)
		return false;
	replica_last_message = GetCurrentTimestamp();
	replica_last_reply = 0;
	replica_stream_lsn = start;
	ereport(LOG, (errmsg("Neon replica streaming committed WAL from %X/%X", LSN_FORMAT_ARGS(start))));
	return replica_reply(true);
}

static bool
replica_receive(void)
{
	for (;;)
	{
		StringInfoData msg;
		int len;
		if (!replica_reply(false) || !PQconsumeInput(replica_conn))
			return false;
		len = PQgetCopyData(replica_conn, &replica_message, 1);
		if (len < 0)
			return false;
		if (len == 0)
		{
			if (TimestampDifferenceExceeds(replica_last_message, GetCurrentTimestamp(), 30000))
				return false;
			replica_wait(WL_SOCKET_READABLE | (PQflush(replica_conn) == 1 ? WL_SOCKET_WRITEABLE : 0));
			continue;
		}
		replica_last_message = GetCurrentTimestamp();
		msg.data = replica_message;
		msg.len = msg.maxlen = len;
		msg.cursor = 1;
		if (msg.data[0] == 'w' && len >= 25)
		{
			XLogRecPtr start = pq_getmsgint64(&msg);
			pq_getmsgint64(&msg);
			pq_getmsgint64(&msg);
			if (start != replica_stream_lsn || len == 25)
				ereport(FATAL, (errmsg("non-contiguous Neon replica WAL stream")));
			replica_message_pos = msg.cursor;
			replica_message_len = len;
			return true;
		}
		if (msg.data[0] != 'k' || len != 18)
			ereport(FATAL, (errmsg("invalid Neon replica WAL protocol message")));
		pq_getmsgint64(&msg);
		pq_getmsgint64(&msg);
		bool requested = pq_getmsgbyte(&msg) != 0;
		PQfreemem(replica_message);
		replica_message = NULL;
		if (requested && !replica_reply(true))
			return false;
	}
}

static int
replica_read_page(XLogReaderState *reader, XLogRecPtr page, int required,
	XLogRecPtr record, char *out, TimeLineID *tli, char *path)
{
	if (!replica_exit_registered)
	{
		on_shmem_exit(replica_disconnect, 0);
		replica_exit_registered = true;
	}
	if (page != replica_page_lsn)
	{
		replica_page_lsn = page;
		replica_page_len = 0;
	}
	while (replica_page_len < required)
	{
		XLogRecPtr need = page + replica_page_len;
		RedoInterruptCallBack();
		if (replica_conn && replica_stream_lsn > need)
			replica_disconnect(0, 0);
		if (!replica_conn && !replica_connect(need))
		{
			replica_disconnect(0, 0);
			replica_wait(0);
			continue;
		}
		if (replica_message_pos == replica_message_len)
		{
			if (replica_message)
				PQfreemem(replica_message);
			replica_message = NULL;
			if (!replica_receive())
			{
				replica_disconnect(0, 0);
				replica_wait(0);
				continue;
			}
		}
		int available = replica_message_len - replica_message_pos;
		int n;
		if (replica_stream_lsn < need)
			n = Min((XLogRecPtr)available, need - replica_stream_lsn);
		else
		{
			n = Min(available, XLOG_BLCKSZ - replica_page_len);
			memcpy(replica_page + replica_page_len, replica_message + replica_message_pos, n);
			replica_page_len += n;
		}
		replica_stream_lsn += n;
		replica_message_pos += n;
	}
	memcpy(out, replica_page, replica_page_len);
	*tli = t_thrd.xlog_cxt.ThisTimeLineID;
	return replica_page_len;
}

void
pg_init_replica(void)
{
	neon_replica_read_page_hook = replica_read_page;
}
