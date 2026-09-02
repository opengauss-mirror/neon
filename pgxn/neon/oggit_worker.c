/*-------------------------------------------------------------------------
 *
 * oggit_worker.c
 *	  Compute-side oggit logical decoding background worker.
 *
 * See oggit_worker.h for the high-level design. In short:
 *
 *   compute start
 *     -> neon.so _PG_init: pg_init_oggit() registers neon.oggit_* GUCs
 *     -> postmaster forks the OGGITWORKER kernel thread role (like WALPROPOSER)
 *        and resolves OggitWorkerMain via load_external_function("neon", ...)
 *          -> OggitWorkerMain (this file):
 *               create logical slot (neon_oggit)
 *               acquire replication slot
 *               CreateDecodingContext(read_page = NeonWALPageRead via hook)
 *               loop:
 *                 XLogReadRecord            (blocks on safekeeper socket)
 *                 LogicalDecodingProcessRecord
 *                   -> neon_oggit plugin -> oggit_write_event (this file)
 *                        -> buffer JSON events
 *                 persist buffered events into oggit.*
 *                 LogicalConfirmReceivedLocation
 *   compute stop -> process exit -> slot released, reader connection closed
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_type.h"
#include "cjson/cJSON.h"
#include "executor/spi.h"
#include "libpq/pqsignal.h"
#include "gssignal/gs_signal.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/postinit.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "neon.h"
#include "oggit_worker.h"

/* ------------------------------------------------------------------------
 * GUCs (set per-endpoint by control_plane in postgresql.conf)
 * ------------------------------------------------------------------------ */
static bool oggit_enabled = false;
static char *oggit_database = NULL;
static char *oggit_effective_slot_name = NULL;
static char *oggit_tenant_id = NULL;
static char *oggit_timeline_id = NULL;
static char *oggit_ancestor_timeline_id = NULL;
static char *oggit_branch_start_lsn = NULL;
static char *oggit_safekeeper_http_urls = NULL;
static int oggit_batch_max_events = 50000;
static int oggit_batch_max_bytes = 256 * 1024 * 1024;

#define OGGIT_SLOT_PREFIX "neon_oggit_slot"

static bool oggit_is_internal_schema(const char *schema);

/* How long to nap when there is transiently nothing to do (ms). The steady
 * state does not poll: XLogReadRecord blocks inside NeonWALPageRead on the
 * safekeeper socket. This nap only bounds ret/error backoff. */
#define OGGIT_IDLE_NAP_MS 1000

/*
 * Waiting for compute_ctl to finish CREATE EXTENSION neon is a normal startup
 * state, not an error. Keep this probe deliberately low-frequency so the
 * extension installer's slow openGauss gs_source/autonomous-transaction path
 * is not competing with a tight ERROR/Abort retry loop.
 */
#define OGGIT_SCHEMA_WAIT_NAP_MS 100

static volatile sig_atomic_t oggit_shutdown_requested = false;
static volatile sig_atomic_t oggit_got_sighup = false;

PG_FUNCTION_INFO_V1(neon_start_oggit_worker);
PG_FUNCTION_INFO_V1(neon_oggit_worker_is_ready);

extern "C" PGDLLEXPORT Datum
neon_start_oggit_worker(PG_FUNCTION_ARGS)
{
	if (!oggit_enabled)
		PG_RETURN_BOOL(false);
	if (g_instance.pid_cxt.OggitWorkerPID != 0 &&
		g_instance.pid_cxt.OggitWorkerReady)
		PG_RETURN_BOOL(true);

	g_instance.pid_cxt.OggitWorkerReady = false;
	SendPostmasterSignal(PMSIGNAL_START_OGGIT_WORKER);
	PG_RETURN_BOOL(true);
}

extern "C" PGDLLEXPORT Datum
neon_oggit_worker_is_ready(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(g_instance.pid_cxt.OggitWorkerPID != 0 &&
				   g_instance.pid_cxt.OggitWorkerReady);
}

typedef struct OggitDecodeSession
{
	LogicalDecodingContext *ctx;
	XLogRecPtr	startptr;
} OggitDecodeSession;

extern char *oggit_deparse_ddl_json_to_string(char *jsonb, char **owner)
__asm__("_Z26deparse_ddl_json_to_stringPcPS_");

/* ------------------------------------------------------------------------
 * Forward declarations
 * ------------------------------------------------------------------------ */
static void oggit_prepare_write(LogicalDecodingContext *ctx, XLogRecPtr lsn,
								TransactionId xid, bool last_write);
static void oggit_do_write(LogicalDecodingContext *ctx, XLogRecPtr lsn,
						   TransactionId xid, bool last_write);
static void oggit_record_event_json(const char *json, int ordinal);
static void oggit_reset_batch(void);
static void oggit_release_resource_owner(ResourceOwner owner,
								 ResourceOwner parent, bool is_commit);
static void oggit_update_required_lsn(XLogRecPtr required_lsn);
static void oggit_report_required_lsn_to_safekeepers(const char *required_lsn);
static bool oggit_get_committed_decode_lsn(XLogRecPtr *committed_lsn);
static void oggit_update_worker_status(const char *status, const char *last_error);

/* ------------------------------------------------------------------------
 * Stage 1: GUC registration
 * ------------------------------------------------------------------------ */
void
pg_init_oggit(void)
{
	DefineCustomBoolVariable(
							 "neon.oggit_enabled",
							 "Enable the compute-side oggit logical decoding worker",
							 NULL,
							 &oggit_enabled,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "neon.oggit_database",
							   "Database the oggit worker connects to for logical decoding",
							   NULL,
							   &oggit_database,
							   "postgres",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "neon.oggit_tenant_id",
							   "Tenant id recorded in oggit.state by the worker",
							   NULL,
							   &oggit_tenant_id,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "neon.oggit_timeline_id",
							   "Timeline id recorded in oggit.state by the worker",
							   NULL,
							   &oggit_timeline_id,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);


	DefineCustomStringVariable(
							   "neon.oggit_ancestor_timeline_id",
							   "Ancestor (parent) timeline id recorded in oggit.state",
							   NULL,
							   &oggit_ancestor_timeline_id,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "neon.oggit_branch_start_lsn",
							   "Branch start LSN (base LSN) recorded in oggit.state",
							   NULL,
							   &oggit_branch_start_lsn,
							   "0/0",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "neon.oggit_safekeeper_http_urls",
							   "Comma-separated safekeeper HTTP base URLs used to report oggit required_lsn",
							   NULL,
							   &oggit_safekeeper_http_urls,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable(
							"neon.oggit_batch_max_events",
							"Maximum number of decoded events buffered in one oggit batch",
							"Zero disables the event-count limit.",
							&oggit_batch_max_events,
							50000, 0, INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable(
							"neon.oggit_batch_max_bytes",
							"Maximum JSON payload bytes buffered in one oggit batch",
							"Zero disables the payload-size limit.",
							&oggit_batch_max_bytes,
							256 * 1024 * 1024, 0, INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);
}

/* ------------------------------------------------------------------------
 * Signal handlers
 * ------------------------------------------------------------------------ */
static void
oggit_shutdown_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	oggit_shutdown_requested = true;
	InterruptPending = true;
	t_thrd.int_cxt.QueryCancelPending = true;
	gs_r_cancel();
	if (t_thrd.proc)
		SetLatch(&t_thrd.proc->procLatch);
	errno = save_errno;
}

static void
oggit_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	oggit_got_sighup = true;
	if (t_thrd.proc)
		SetLatch(&t_thrd.proc->procLatch);
	errno = save_errno;
}

/* ------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------ */
static bool
oggit_guc_present(const char *value)
{
	return value != NULL && value[0] != '\0';
}

static void
oggit_initialize_effective_slot_name(void)
{
	char		database_oid_suffix[32];

	snprintf(database_oid_suffix, sizeof(database_oid_suffix), "_%u",
			 u_sess->proc_cxt.MyDatabaseId);
	if (strlen(OGGIT_SLOT_PREFIX) + strlen(database_oid_suffix) >= NAMEDATALEN)
		elog(ERROR,
			 "oggit: fixed slot prefix %s is too long for database-specific suffix %s",
			 OGGIT_SLOT_PREFIX, database_oid_suffix);

	oggit_effective_slot_name = psprintf("%s%s", OGGIT_SLOT_PREFIX, database_oid_suffix);
}

static char *
oggit_lsn_to_string(XLogRecPtr lsn)
{
	return psprintf("%X/%X", LSN_FORMAT_ARGS(lsn));
}

static char *
oggit_trim(char *value)
{
	char	   *end;

	while (*value && isspace((unsigned char) *value))
		value++;
	end = value + strlen(value);
	while (end > value && isspace((unsigned char) *(end - 1)))
		*(--end) = '\0';
	return value;
}

static bool
oggit_parse_http_url(const char *url, char **host, char **port)
{
	char	   *copy;
	char	   *work;
	char	   *slash;
	char	   *colon;

	if (!oggit_guc_present(url))
		return false;

	copy = pstrdup(url);
	work = oggit_trim(copy);
	if (strncmp(work, "http://", strlen("http://")) == 0)
		work += strlen("http://");

	slash = strchr(work, '/');
	if (slash != NULL)
		*slash = '\0';
	colon = strrchr(work, ':');
	if (colon != NULL)
	{
		*colon = '\0';
		*host = pstrdup(work);
		*port = pstrdup(colon + 1);
	}
	else
	{
		*host = pstrdup(work);
		*port = pstrdup("80");
	}
	pfree(copy);
	return (*host)[0] != '\0' && (*port)[0] != '\0';
}

static bool
oggit_send_all(int fd, const char *buf, size_t len)
{
	size_t		sent = 0;

	while (sent < len)
	{
		ssize_t		rc = send(fd, buf + sent, len - sent, 0);

		if (rc <= 0)
			return false;
		sent += rc;
	}
	return true;
}

static bool
oggit_parse_lsn(const char *value, XLogRecPtr *lsn)
{
	uint32		hi;
	uint32		lo;

	if (value == NULL || sscanf(value, "%X/%X", &hi, &lo) != 2)
		return false;
	*lsn = ((uint64) hi << 32) | lo;
	return true;
}

static bool
oggit_http_get_timeline_status(const char *base_url, XLogRecPtr *safe_lsn)
{
	char	   *host = NULL;
	char	   *port = NULL;
	char	   *path = NULL;
	char	   *request = NULL;
	struct addrinfo hints;
	struct addrinfo *result = NULL;
	struct addrinfo *rp;
	int			fd = -1;
	bool		ok = false;
	StringInfoData response;
	char		buffer[2048];
	ssize_t		nread;
	struct timeval timeout;
	char	   *body;
	cJSON	   *status = NULL;
	cJSON	   *commit_item;
	cJSON	   *flush_item;
	XLogRecPtr	commit_lsn;
	XLogRecPtr	flush_lsn;

	initStringInfo(&response);
	if (!oggit_parse_http_url(base_url, &host, &port))
		goto done;

	path = psprintf("/v1/tenant/%s/timeline/%s", oggit_tenant_id, oggit_timeline_id);
	request = psprintf("GET %s HTTP/1.1\r\n"
					   "Host: %s:%s\r\n"
					   "Accept: application/json\r\n"
					   "Connection: close\r\n"
					   "\r\n",
					   path, host, port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	if (getaddrinfo(host, port, &hints, &result) != 0)
		goto done;

	for (rp = result; rp != NULL; rp = rp->ai_next)
	{
		fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (fd < 0)
			continue;

		timeout.tv_sec = 2;
		timeout.tv_usec = 0;
		(void) setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
		(void) setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

		if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0)
			break;
		close(fd);
		fd = -1;
	}
	if (fd < 0 || !oggit_send_all(fd, request, strlen(request)))
		goto done;

	while ((nread = recv(fd, buffer, sizeof(buffer), 0)) > 0)
		appendBinaryStringInfo(&response, buffer, nread);
	if (response.len == 0 ||
		(strncmp(response.data, "HTTP/1.1 2", strlen("HTTP/1.1 2")) != 0 &&
		 strncmp(response.data, "HTTP/1.0 2", strlen("HTTP/1.0 2")) != 0))
		goto done;

	body = strstr(response.data, "\r\n\r\n");
	if (body == NULL)
		goto done;
	body += 4;
	status = cJSON_Parse(body);
	if (status == NULL)
		goto done;

	commit_item = cJSON_GetObjectItemCaseSensitive(status, "commit_lsn");
	flush_item = cJSON_GetObjectItemCaseSensitive(status, "flush_lsn");
	if (!cJSON_IsString(commit_item) || !cJSON_IsString(flush_item) ||
		!oggit_parse_lsn(commit_item->valuestring, &commit_lsn) ||
		!oggit_parse_lsn(flush_item->valuestring, &flush_lsn))
		goto done;

	*safe_lsn = XLByteLT(commit_lsn, flush_lsn) ? commit_lsn : flush_lsn;
	ok = true;

done:
	if (status != NULL)
		cJSON_Delete(status);
	if (fd >= 0)
		close(fd);
	if (result != NULL)
		freeaddrinfo(result);
	if (host != NULL)
		pfree(host);
	if (port != NULL)
		pfree(port);
	if (path != NULL)
		pfree(path);
	if (request != NULL)
		pfree(request);
	pfree(response.data);
	return ok;
}

static bool
oggit_get_committed_decode_lsn(XLogRecPtr *committed_lsn)
{
	static XLogRecPtr last_committed_lsn = InvalidXLogRecPtr;
	char	   *urls;
	char	   *saveptr = NULL;
	char	   *url;
	XLogRecPtr	best_lsn = InvalidXLogRecPtr;
	bool		found = false;

	if (!oggit_guc_present(oggit_safekeeper_http_urls) ||
		!oggit_guc_present(oggit_tenant_id) ||
		!oggit_guc_present(oggit_timeline_id))
		return false;

	urls = pstrdup(oggit_safekeeper_http_urls);
	for (url = strtok_r(urls, ",", &saveptr);
		 url != NULL;
		 url = strtok_r(NULL, ",", &saveptr))
	{
		XLogRecPtr	candidate;

		url = oggit_trim(url);
		if (url[0] == '\0')
			continue;
		if (!oggit_http_get_timeline_status(url, &candidate))
		{
			elog(WARNING, "oggit: failed to read commit_lsn from safekeeper %s", url);
			continue;
		}
		if (!found || XLByteLT(best_lsn, candidate))
			best_lsn = candidate;
		found = true;
	}
	pfree(urls);

	if (!found)
		return false;
	if (XLByteEQ(last_committed_lsn, InvalidXLogRecPtr) ||
		XLByteLT(last_committed_lsn, best_lsn))
		last_committed_lsn = best_lsn;
	*committed_lsn = last_committed_lsn;
	return true;
}

static bool
oggit_http_put_required_lsn(const char *base_url, const char *required_lsn)
{
	char	   *host = NULL;
	char	   *port = NULL;
	char	   *body;
	char	   *path;
	char	   *request;
	struct addrinfo hints;
	struct addrinfo *result = NULL;
	struct addrinfo *rp;
	int			fd = -1;
	bool		ok = false;
	char		response[256];
	ssize_t		nread;
	struct timeval timeout;

	if (!oggit_parse_http_url(base_url, &host, &port))
		return false;

	body = psprintf("{\"oggit_required_lsn\":\"%s\"}", required_lsn);
	path = psprintf("/v1/tenant/%s/timeline/%s/oggit_required_lsn",
				   oggit_tenant_id, oggit_timeline_id);
	request = psprintf("PUT %s HTTP/1.1\r\n"
					   "Host: %s:%s\r\n"
					   "Content-Type: application/json\r\n"
					   "Content-Length: %zu\r\n"
					   "Connection: close\r\n"
					   "\r\n"
					   "%s",
					   path, host, port, strlen(body), body);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	if (getaddrinfo(host, port, &hints, &result) != 0)
		goto done;

	for (rp = result; rp != NULL; rp = rp->ai_next)
	{
		fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (fd < 0)
			continue;

		timeout.tv_sec = 2;
		timeout.tv_usec = 0;
		(void) setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
		(void) setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

		if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0)
			break;
		close(fd);
		fd = -1;
	}
	if (fd < 0)
		goto done;

	if (!oggit_send_all(fd, request, strlen(request)))
		goto done;

	nread = recv(fd, response, sizeof(response) - 1, 0);
	if (nread <= 0)
		goto done;
	response[nread] = '\0';
	ok = (strncmp(response, "HTTP/1.1 2", strlen("HTTP/1.1 2")) == 0 ||
		  strncmp(response, "HTTP/1.0 2", strlen("HTTP/1.0 2")) == 0);

done:
	if (fd >= 0)
		close(fd);
	if (result != NULL)
		freeaddrinfo(result);
	if (host)
		pfree(host);
	if (port)
		pfree(port);
	pfree(body);
	pfree(path);
	pfree(request);
	return ok;
}

static void
oggit_report_required_lsn_to_safekeepers(const char *required_lsn)
{
	char	   *urls;
	char	   *saveptr = NULL;
	char	   *url;

	if (!oggit_guc_present(oggit_safekeeper_http_urls) ||
		!oggit_guc_present(oggit_tenant_id) ||
		!oggit_guc_present(oggit_timeline_id))
		return;

	urls = pstrdup(oggit_safekeeper_http_urls);
	for (url = strtok_r(urls, ",", &saveptr);
		 url != NULL;
		 url = strtok_r(NULL, ",", &saveptr))
	{
		url = oggit_trim(url);
		if (url[0] == '\0')
			continue;
		if (!oggit_http_put_required_lsn(url, required_lsn))
			elog(WARNING, "oggit: failed to report required_lsn %s to safekeeper %s",
				 required_lsn, url);
	}
	pfree(urls);
}

static bool
oggit_state_table_exists(void)
{
	bool		exists;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "oggit: SPI_connect failed while probing oggit.state");

	if (SPI_execute("SELECT 1 FROM pg_class c JOIN pg_namespace n "
					"ON n.oid = c.relnamespace "
					"WHERE n.nspname = 'oggit' AND c.relname = 'state'",
					true, 1) < 0)
		elog(ERROR, "oggit: failed to probe oggit.state");
	exists = (SPI_processed > 0);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	return exists;
}

static bool
oggit_wait_for_state_table(void)
{
	bool		reported_wait = false;

	while (!oggit_shutdown_requested)
	{
		if (oggit_state_table_exists())
			return true;

		if (!reported_wait)
		{
			elog(LOG, "oggit worker: waiting for neon extension to create oggit.state");
			reported_wait = true;
		}

		(void) WaitLatch(&t_thrd.proc->procLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						 OGGIT_SCHEMA_WAIT_NAP_MS);
		ResetLatch(&t_thrd.proc->procLatch);
		CHECK_FOR_INTERRUPTS();
	}

	return false;
}

/*
 * Run one SQL statement through SPI inside the current transaction. Used for
 * the idempotent bootstrap DDL. Errors propagate as ereport(ERROR).
 */
static void
oggit_spi_exec(const char *sql)
{
	int			rc = SPI_execute(sql, false, 0);

	if (rc < 0)
		elog(ERROR, "oggit: SPI_execute failed (%d) for: %s", rc, sql);
}

/*
 * Seed oggit.state with the branch identity provided via GUCs, and ensure the
 * oggit publication exists. This mirrors what the old control_plane worker did
 * through oggit_init_state().
 *
 * The worker does NOT create the neon extension: compute_ctl owns extension
 * creation (compute_tools spec_apply runs "CREATE EXTENSION neon WITH SCHEMA
 * neon"). Running CREATE EXTENSION here is both racy and unsafe -- the install
 * script creates plpgsql functions, whose validator path (InsertGsSource ->
 * maskPassword) crashed the worker in early startup. Instead we wait until
 * compute_ctl has created oggit.state, then proceed.
 *
 * Must be called inside a transaction with SPI connected.
 */
static void
oggit_bootstrap_state(void)
{
	StringInfoData buf;
	const char *ancestor_sql;

	/*
	 * Wait for compute_ctl to have created the neon extension (which ships the
	 * oggit metadata schema/tables in neon--1.0.sql). If oggit.state is not
	 * present yet, raise a retryable error; the worker's outer loop naps and
	 * retries.
	 */
	if (SPI_execute("SELECT 1 FROM pg_class c JOIN pg_namespace n "
					"ON n.oid = c.relnamespace "
					"WHERE n.nspname = 'oggit' AND c.relname = 'state'",
					true, 1) < 0)
		elog(ERROR, "oggit: failed to probe oggit.state");
	if (SPI_processed == 0)
		elog(ERROR, "oggit: neon extension/oggit schema not present yet, will retry");

	oggit_spi_exec("ALTER TABLE oggit.state ADD COLUMN IF NOT EXISTS database_name text");
	oggit_spi_exec("ALTER TABLE oggit.state ADD COLUMN IF NOT EXISTS database_oid oid");
	if (SPI_execute("SELECT database_name, database_oid::text FROM oggit.state WHERE id = true",
					true, 1) < 0)
		elog(ERROR, "oggit: failed to read database identity from oggit.state");
	if (SPI_processed > 0)
	{
		char	   *stored_database = SPI_getvalue(SPI_tuptable->vals[0],
											 SPI_tuptable->tupdesc, 1);
		char	   *stored_database_oid = SPI_getvalue(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 2);

		if (stored_database != NULL && stored_database[0] != '\0' &&
			strcmp(stored_database, u_sess->proc_cxt.MyProcPort->database_name) != 0)
			elog(ERROR,
				 "oggit.state belongs to database %s, current database is %s",
				 stored_database, u_sess->proc_cxt.MyProcPort->database_name);
		if (stored_database_oid != NULL && stored_database_oid[0] != '\0' &&
			(Oid) strtoul(stored_database_oid, NULL, 10) !=
				u_sess->proc_cxt.MyDatabaseId)
			elog(ERROR,
				 "oggit.state belongs to database oid %s, current database oid is %u",
				 stored_database_oid, u_sess->proc_cxt.MyDatabaseId);
	}

	/*
	 * openGauss logical DDL decoding requires a publication created WITH
	 * (ddl='all'). Create it only if missing. Use a plain existence check +
	 * CREATE rather than a plpgsql DO block to avoid the function-validator
	 * code path.
	 */
	if (SPI_execute("SELECT 1 FROM pg_publication WHERE pubname = 'neon_oggit_pub'",
					true, 1) < 0)
		elog(ERROR, "oggit: failed to probe neon_oggit_pub");
	if (SPI_processed == 0)
		oggit_spi_exec("CREATE PUBLICATION neon_oggit_pub FOR ALL TABLES WITH (ddl='all')");

	ancestor_sql = oggit_guc_present(oggit_ancestor_timeline_id)
		? quote_literal_cstr(oggit_ancestor_timeline_id) : "NULL";

	/*
	 * Manual upsert of the single-row oggit.state. Historically a boolean PK
	 * row with id=true has been observed more than once despite CHECK/PK;
	 * heal that first so subsequent UPDATEs cannot race duplicate keys.
	 * required_lsn/decode_lsn are only seeded on first insert; the decode
	 * loop advances them afterwards.
	 */
	if (SPI_execute("SELECT count(*)::text FROM oggit.state WHERE id = true",
					true, 1) < 0)
		elog(ERROR, "oggit: failed to count oggit.state rows");
	if (SPI_processed != 1 || SPI_tuptable == NULL)
		elog(ERROR, "oggit: unexpected count result for oggit.state");

	{
		char	   *count_text = SPI_getvalue(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1);
		long		nrow = count_text != NULL ? strtol(count_text, NULL, 10) : 0;

		if (nrow > 1)
		{
			/*
			 * Keep the most advanced row and drop the rest. Ties fall back to
			 * the smallest ctid so the survivor is deterministic.
			 */
			oggit_spi_exec(
				"DELETE FROM oggit.state "
				"WHERE id = true AND ctid <> ("
				"  SELECT ctid FROM oggit.state WHERE id = true "
				"  ORDER BY decode_lsn DESC NULLS LAST, "
				"           confirmed_lsn DESC NULLS LAST, "
				"           scanned_lsn DESC NULLS LAST, "
				"           updated_at DESC NULLS LAST, "
				"           ctid ASC "
				"  LIMIT 1)");
			elog(LOG, "oggit: healed duplicate oggit.state rows, kept 1 of %ld",
				 nrow);
		}
	}

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "UPDATE oggit.state SET "
					 "  tenant_id = %s, timeline_id = %s, database_name = %s, database_oid = %u, "
					 "  ancestor_timeline_id = %s, branch_start_lsn = %s, "
					 "  slot_name = %s, status = 'active', last_error = NULL, "
					 "  updated_at = now() WHERE id = true",
					 quote_literal_cstr(oggit_tenant_id),
					 quote_literal_cstr(oggit_timeline_id),
					 quote_literal_cstr(u_sess->proc_cxt.MyProcPort->database_name),
					 u_sess->proc_cxt.MyDatabaseId,
					 ancestor_sql,
					 quote_literal_cstr(oggit_branch_start_lsn),
					 quote_literal_cstr(oggit_effective_slot_name));
	oggit_spi_exec(buf.data);

	if (SPI_processed == 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "INSERT INTO oggit.state "
						 "(id, tenant_id, timeline_id, database_name, database_oid, ancestor_timeline_id, "
						 " branch_start_lsn, slot_name, required_lsn, decode_lsn, scanned_lsn, "
						 " status, updated_at) "
						 "VALUES (true, %s, %s, %s, %u, %s, %s, %s, %s, %s, %s, 'active', now())",
						 quote_literal_cstr(oggit_tenant_id),
						 quote_literal_cstr(oggit_timeline_id),
						 quote_literal_cstr(u_sess->proc_cxt.MyProcPort->database_name),
						 u_sess->proc_cxt.MyDatabaseId,
						 ancestor_sql,
						 quote_literal_cstr(oggit_branch_start_lsn),
						 quote_literal_cstr(oggit_effective_slot_name),
						 quote_literal_cstr(oggit_branch_start_lsn),
						 quote_literal_cstr(oggit_branch_start_lsn),
						 quote_literal_cstr(oggit_branch_start_lsn));
		oggit_spi_exec(buf.data);
	}

	pfree(buf.data);
}

/*
 * Create the logical replication slot with the neon_oggit plugin if it does
 * not exist yet. Runs its own snapshot-consistent init (openGauss handles the
 * startpoint search inside create_logical_replication_slot()).
 *
 * Must be called inside a transaction.
 */
static bool
oggit_slot_exists_and_matches(void)
{
	int			rc;
	Oid			argtypes[1];
	Datum		values[1];
	char	   *plugin;
	char	   *slot_type;
	char	   *database_oid;

	argtypes[0] = TEXTOID;
	values[0] = CStringGetTextDatum(oggit_effective_slot_name);

	rc = SPI_execute_with_args(
							   "SELECT plugin::text, slot_type::text, datoid::text "
							   "FROM pg_replication_slots WHERE slot_name = $1",
							   1, argtypes, values, NULL, true, 1, NULL);
	if (rc != SPI_OK_SELECT)
		elog(ERROR, "oggit: failed to probe replication slot %s",
			 oggit_effective_slot_name);
	if (SPI_processed == 0)
		return false;

	plugin = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	slot_type = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	database_oid = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	if (plugin == NULL || strcmp(plugin, "neon_oggit") != 0 ||
		slot_type == NULL || strcmp(slot_type, "logical") != 0 ||
		database_oid == NULL ||
		(Oid) strtoul(database_oid, NULL, 10) != u_sess->proc_cxt.MyDatabaseId)
		elog(ERROR,
			 "oggit: replication slot %s does not match plugin neon_oggit and database oid %u",
			 oggit_effective_slot_name, u_sess->proc_cxt.MyDatabaseId);

	return true;
}

static void
oggit_ensure_slot(void)
{
	if (oggit_slot_exists_and_matches())
		return;

	/*
	 * Use the SQL entry point to create the logical slot. This keeps the
	 * startpoint / snapshot bootstrap identical to the manual path used by
	 * the tests and the old worker.
	 */
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT pg_create_logical_replication_slot(%s, 'neon_oggit')",
						 quote_literal_cstr(oggit_effective_slot_name));
		PG_TRY();
		{
			oggit_spi_exec(buf.data);
		}
		PG_CATCH();
		{
			if (t_thrd.slot_cxt.MyReplicationSlot != NULL)
				ReplicationSlotRelease();
			PG_RE_THROW();
		}
		PG_END_TRY();
		pfree(buf.data);
	}

	if (t_thrd.slot_cxt.MyReplicationSlot != NULL)
		ReplicationSlotRelease();
}

/* ------------------------------------------------------------------------
 * Output plugin writer callbacks
 *
 * The neon_oggit plugin produces one compact JSON object per event into
 * ctx->out. prepare_write resets the buffer; do_write hands the completed
 * JSON to the persistence routine.
 * ------------------------------------------------------------------------ */
/*
 * Events decoded within one batch are buffered here as palloc'd JSON strings
 * (in oggit_batch_cxt), then persisted through SPI after decoding yields for
 * the batch. The LogicalDecodingContext itself is long-lived across batches,
 * so reorderbuffer spill state for uncommitted transactions is not discarded.
 */
static MemoryContext oggit_decode_cxt = NULL;
static MemoryContext oggit_batch_cxt = NULL;
/* Long-lived worker allocations that must survive AbortOutOfAnyTransaction /
 * FlushErrorState (e.g. last_error copied in PG_CATCH). */
static MemoryContext oggit_worker_cxt = NULL;
static List *oggit_event_buf = NIL;
static uint64 oggit_batch_event_count = 0;
static uint64 oggit_batch_payload_bytes = 0;
/* Max commit LSNs observed while buffering/persisting the current batch.
 * Applied once in oggit_persist_batch() instead of per-commit UPDATEs. */
static XLogRecPtr oggit_batch_max_decode_lsn = InvalidXLogRecPtr;
static XLogRecPtr oggit_batch_max_confirmed_lsn = InvalidXLogRecPtr;

static bool
oggit_batch_limit_reached(void)
{
	bool		event_limit_reached;
	bool		byte_limit_reached;

	event_limit_reached = oggit_batch_max_events > 0 &&
		oggit_batch_event_count >= (uint64) oggit_batch_max_events;
	byte_limit_reached = oggit_batch_max_bytes > 0 &&
		oggit_batch_payload_bytes >= (uint64) oggit_batch_max_bytes;

	return event_limit_reached || byte_limit_reached;
}

static void
oggit_reset_batch(void)
{
	if (oggit_batch_cxt != NULL)
		MemoryContextReset(oggit_batch_cxt);

	oggit_event_buf = NIL;
	oggit_batch_event_count = 0;
	oggit_batch_payload_bytes = 0;
	oggit_batch_max_decode_lsn = InvalidXLogRecPtr;
	oggit_batch_max_confirmed_lsn = InvalidXLogRecPtr;
}

static void
oggit_note_batch_commit_lsn(const char *decode_lsn_str, const char *confirmed_lsn_str)
{
	bool		have_error = false;
	XLogRecPtr	decode_lsn;
	XLogRecPtr	confirmed_lsn;

	if (decode_lsn_str != NULL && decode_lsn_str[0] != '\0')
	{
		have_error = false;
		decode_lsn = pg_lsn_in_internal(decode_lsn_str, &have_error);
		if (!have_error &&
			(XLByteEQ(oggit_batch_max_decode_lsn, InvalidXLogRecPtr) ||
			 XLByteLT(oggit_batch_max_decode_lsn, decode_lsn)))
			oggit_batch_max_decode_lsn = decode_lsn;
	}

	if (confirmed_lsn_str != NULL && confirmed_lsn_str[0] != '\0')
	{
		have_error = false;
		confirmed_lsn = pg_lsn_in_internal(confirmed_lsn_str, &have_error);
		if (!have_error &&
			(XLByteEQ(oggit_batch_max_confirmed_lsn, InvalidXLogRecPtr) ||
			 XLByteLT(oggit_batch_max_confirmed_lsn, confirmed_lsn)))
			oggit_batch_max_confirmed_lsn = confirmed_lsn;
	}
}

static void
oggit_release_resource_owner(ResourceOwner owner, ResourceOwner parent,
							 bool is_commit)
{
	if (owner == NULL)
		return;

	Assert(t_thrd.utils_cxt.CurrentResourceOwner == owner);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, true);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true);
	t_thrd.utils_cxt.CurrentResourceOwner = parent;
	ResourceOwnerDelete(owner);
}

static void
oggit_prepare_write(LogicalDecodingContext *ctx, XLogRecPtr lsn,
					TransactionId xid, bool last_write)
{
	resetStringInfo(ctx->out);
}

static void
oggit_do_write(LogicalDecodingContext *ctx, XLogRecPtr lsn,
			   TransactionId xid, bool last_write)
{
	MemoryContext old;

	if (ctx->out->len == 0)
		return;

	old = MemoryContextSwitchTo(oggit_batch_cxt);
	oggit_event_buf = lappend(oggit_event_buf, pstrdup(ctx->out->data));
	MemoryContextSwitchTo(old);

	oggit_batch_event_count++;
	oggit_batch_payload_bytes += (uint64) ctx->out->len;
}

/* ------------------------------------------------------------------------
 * Event persistence (moved from the Rust oggit_record_event)
 * ------------------------------------------------------------------------ */

static const char *
oggit_json_str(const cJSON *obj, const char *field)
{
	const cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, field);

	if (item != NULL && cJSON_IsString(item))
		return item->valuestring;
	return NULL;
}

static bool
oggit_json_bool_or_false(cJSON *obj, const char *field)
{
	cJSON	   *item = cJSON_GetObjectItemCaseSensitive(obj, field);

	return item != NULL && cJSON_IsTrue(item);
}

static char *
oggit_truncate_replay_sql(cJSON *event, const char **first_schema,
						  const char **first_table)
{
	cJSON	   *relations = cJSON_GetObjectItemCaseSensitive(event, "relations");
	cJSON	   *rel;
	StringInfoData sql;
	bool		have_relation = false;

	if (first_schema != NULL)
		*first_schema = NULL;
	if (first_table != NULL)
		*first_table = NULL;
	if (relations == NULL || !cJSON_IsArray(relations))
		return NULL;

	initStringInfo(&sql);
	appendStringInfoString(&sql, "TRUNCATE TABLE ");
	cJSON_ArrayForEach(rel, relations)
	{
		const char *schema = oggit_json_str(rel, "schema");
		const char *table = oggit_json_str(rel, "table");

		if (schema == NULL || table == NULL || oggit_is_internal_schema(schema))
			continue;

		if (!have_relation)
		{
			if (first_schema != NULL)
				*first_schema = schema;
			if (first_table != NULL)
				*first_table = table;
		}
		else
			appendStringInfoString(&sql, ", ");

		appendStringInfo(&sql, "%s.%s", quote_identifier(schema),
						 quote_identifier(table));
		have_relation = true;
	}

	if (!have_relation)
	{
		pfree(sql.data);
		return NULL;
	}

	if (oggit_json_bool_or_false(event, "restart_seqs"))
		appendStringInfoString(&sql, " RESTART IDENTITY");
	if (oggit_json_bool_or_false(event, "cascade"))
		appendStringInfoString(&sql, " CASCADE");

	return sql.data;
}

/*
 * Extract the commit-ish LSN string from an event, matching the Rust
 * oggit_event_lsn() precedence.
 */
static const char *
oggit_event_lsn(const cJSON *event)
{
	static const char *const keys[] = {
		"commit_lsn", "change_lsn", "message_lsn", "callback_commit_lsn"
	};
	int			i;

	for (i = 0; i < (int) (sizeof(keys) / sizeof(keys[0])); i++)
	{
		const char *v = oggit_json_str(event, keys[i]);

		if (v != NULL)
			return v;
	}
	return "0/0";
}

/*
 * Copy an event's commit grouping key without retaining any cJSON-owned
 * memory. Malformed events use the same fallback key as oggit_event_lsn().
 */
static void
oggit_event_commit_lsn(const char *json, char *commit_lsn, size_t size)
{
	cJSON	   *event;
	const char *event_lsn = "0/0";

	event = cJSON_Parse(json);
	if (event != NULL)
		event_lsn = oggit_event_lsn(event);

	strlcpy(commit_lsn, event_lsn, size);

	if (event != NULL)
		cJSON_Delete(event);
}

static bool
oggit_is_internal_schema(const char *schema)
{
	return schema != NULL &&
		(strcmp(schema, "oggit") == 0 ||
		 strcmp(schema, "_oggit") == 0 ||
		 strcmp(schema, "neon") == 0 ||
		 strcmp(schema, "coverage") == 0 ||
		 strcmp(schema, "oggit_fdw") == 0 ||
		 strncmp(schema, "neon_", 5) == 0);
}

static bool
oggit_identifier_mentions_internal_schema(const char *raw)
{
	char	   *copy;
	char	   *start;
	char	   *end;
	char	   *dot;
	bool		matches;

	if (raw == NULL)
		return false;

	copy = pstrdup(raw);
	start = copy;
	while (*start && isspace((unsigned char) *start))
		start++;
	while (*start == '"' || *start == '\'' || *start == '`')
		start++;

	end = start + strlen(start);
	while (end > start &&
		   (isspace((unsigned char) end[-1]) ||
			end[-1] == '"' || end[-1] == '\'' || end[-1] == '`' ||
			end[-1] == ';' || end[-1] == ',' || end[-1] == ')'))
		*--end = '\0';

	dot = strchr(start, '.');
	if (dot != NULL)
		*dot = '\0';

	matches = oggit_is_internal_schema(start);
	pfree(copy);
	return matches;
}

static bool
oggit_sql_mentions_internal_schema_ddl(const char *sql)
{
	char	   *copy;
	char	   *token;
	char	   *saveptr = NULL;
	const char *schema_name = NULL;
	bool		seen_create_or_drop = false;
	bool		seen_schema = false;
	bool		matches = false;

	if (sql == NULL)
		return false;

	copy = pstrdup(sql);
	for (token = strtok_r(copy, " \t\r\n(", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, " \t\r\n(", &saveptr))
	{
		if (!seen_create_or_drop)
		{
			if (pg_strcasecmp(token, "CREATE") == 0 ||
				pg_strcasecmp(token, "DROP") == 0)
				seen_create_or_drop = true;
			else
				break;
			continue;
		}
		if (!seen_schema)
		{
			if (pg_strcasecmp(token, "SCHEMA") != 0)
				break;
			seen_schema = true;
			continue;
		}
		if (pg_strcasecmp(token, "IF") == 0 ||
			pg_strcasecmp(token, "NOT") == 0 ||
			pg_strcasecmp(token, "EXISTS") == 0)
			continue;
		schema_name = token;
		break;
	}

	if (schema_name != NULL)
		matches = oggit_identifier_mentions_internal_schema(schema_name);
	pfree(copy);
	return matches;
}

static bool
oggit_json_mentions_internal_schema(const cJSON *value)
{
	const cJSON *child;
	const char *fmt;
	const char *objtype;
	const char *name;
	bool		describes_schema = false;

	if (value == NULL)
		return false;
	if (cJSON_IsString(value))
		return oggit_sql_mentions_internal_schema_ddl(value->valuestring);
	if (cJSON_IsArray(value))
	{
		cJSON_ArrayForEach(child, value)
		{
			if (oggit_json_mentions_internal_schema(child))
				return true;
		}
		return false;
	}
	if (cJSON_IsObject(value))
	{
		fmt = oggit_json_str(value, "fmt");
		objtype = oggit_json_str(value, "objtype");
		name = oggit_json_str(value, "name");
		if (fmt != NULL)
		{
			char	   *upper_fmt = pstrdup(fmt);
			int			i;

			for (i = 0; upper_fmt[i]; i++)
				upper_fmt[i] = pg_toupper((unsigned char) upper_fmt[i]);
			describes_schema = strstr(upper_fmt, "SCHEMA") != NULL;
			pfree(upper_fmt);
		}
		if (objtype != NULL && pg_strcasecmp(objtype, "schema") == 0)
			describes_schema = true;
		if (describes_schema && oggit_identifier_mentions_internal_schema(name))
			return true;

		cJSON_ArrayForEach(child, value)
		{
			if (child->string != NULL &&
				(strcmp(child->string, "schemaname") == 0 ||
				 strcmp(child->string, "schema") == 0) &&
				cJSON_IsString(child) &&
				oggit_identifier_mentions_internal_schema(child->valuestring))
				return true;
			if (child->string != NULL &&
				strcmp(child->string, "objidentity") == 0 &&
				cJSON_IsString(child) &&
				strchr(child->valuestring, '.') != NULL &&
				oggit_identifier_mentions_internal_schema(child->valuestring))
				return true;
			if (oggit_json_mentions_internal_schema(child))
				return true;
		}
	}
	return false;
}

static bool
oggit_string_mentions_merge_barrier(const char *value)
{
	return value != NULL &&
		(strstr(value, "oggit_merge_write_barrier") != NULL ||
		 strstr(value, "enforce_merge_write_barrier") != NULL ||
		 strstr(value, "merge_write_barrier") != NULL);
}

static bool
oggit_json_mentions_merge_barrier(const cJSON *value)
{
	const cJSON *child;

	if (value == NULL)
		return false;
	if (cJSON_IsString(value))
		return oggit_string_mentions_merge_barrier(value->valuestring);
	if (cJSON_IsArray(value) || cJSON_IsObject(value))
	{
		cJSON_ArrayForEach(child, value)
		{
			if (oggit_json_mentions_merge_barrier(child))
				return true;
		}
	}
	return false;
}

static const char *
oggit_json_nested_str(const cJSON *obj, const char *field, const char *nested)
{
	const cJSON *parent = cJSON_GetObjectItemCaseSensitive(obj, field);

	if (parent != NULL && cJSON_IsObject(parent))
		return oggit_json_str(parent, nested);
	return NULL;
}

static const char *
oggit_ddl_object_type(const char *raw_type, const char *upper_sql)
{
	/* A column rename is deparsed as ALTER TABLE, so SQL shape is more
	 * specific than the payload's relation-level objtype. */
	if (upper_sql != NULL && strstr(upper_sql, "RENAME COLUMN") != NULL)
		return "COLUMN";

	if (raw_type != NULL)
	{
		char	   *upper_type = pstrdup(raw_type);
		const char *object_type = "OTHER";
		int			i;

		for (i = 0; upper_type[i]; i++)
			upper_type[i] = pg_toupper((unsigned char) upper_type[i]);

		if (strcmp(upper_type, "TABLE") == 0)
			object_type = "TABLE";
		else if (strcmp(upper_type, "COLUMN") == 0)
			object_type = "COLUMN";
		else if (strcmp(upper_type, "INDEX") == 0)
			object_type = "INDEX";
		else if (strcmp(upper_type, "CONSTRAINT") == 0 ||
				 strcmp(upper_type, "TABLE CONSTRAINT") == 0)
			object_type = "CONSTRAINT";
		else if (strcmp(upper_type, "SEQUENCE") == 0 ||
				 strcmp(upper_type, "LARGE SEQUENCE") == 0)
			object_type = "SEQUENCE";
		else if (strcmp(upper_type, "TRIGGER") == 0)
			object_type = "TRIGGER";
		else if (strcmp(upper_type, "FUNCTION") == 0)
			object_type = "FUNCTION";
		else if (strcmp(upper_type, "VIEW") == 0)
			object_type = "VIEW";
		else if (strcmp(upper_type, "RULE") == 0)
			object_type = "RULE";
		else if (strcmp(upper_type, "SCHEMA") == 0)
			object_type = "SCHEMA";

		pfree(upper_type);
		if (strcmp(object_type, "OTHER") != 0)
			return object_type;
	}

	if (upper_sql != NULL && strstr(upper_sql, "TRIGGER") != NULL)
		return "TRIGGER";
	if (upper_sql != NULL && strstr(upper_sql, "FUNCTION") != NULL)
		return "FUNCTION";
	if (upper_sql != NULL && strstr(upper_sql, "VIEW") != NULL)
		return "VIEW";
	if (upper_sql != NULL && strstr(upper_sql, "SEQUENCE") != NULL)
		return "SEQUENCE";
	if (upper_sql != NULL && strstr(upper_sql, "RULE") != NULL)
		return "RULE";
	if (upper_sql != NULL && (strstr(upper_sql, "ADD CONSTRAINT") != NULL ||
							  strstr(upper_sql, "CHECK") != NULL ||
							  strstr(upper_sql, "FOREIGN KEY") != NULL))
		return "CONSTRAINT";
	if (upper_sql != NULL && strstr(upper_sql, "INDEX") != NULL)
		return "INDEX";
	if (upper_sql != NULL && strstr(upper_sql, "TABLE") != NULL)
		return "TABLE";
	if (upper_sql != NULL && strstr(upper_sql, "SCHEMA") != NULL)
		return "SCHEMA";
	return "OTHER";
}

static bool
oggit_ddl_identity_from_sql(const char *sql, const char *object_type,
							char **identity_copy)
{
	char	   *upper_sql;
	char	   *marker;
	const char *identity_start;
	const char *identity_end;
	bool		quoted = false;
	int			i;

	if (sql == NULL || object_type == NULL || identity_copy == NULL)
		return false;

	upper_sql = pstrdup(sql);
	for (i = 0; upper_sql[i]; i++)
		upper_sql[i] = pg_toupper((unsigned char) upper_sql[i]);

	marker = strstr(upper_sql, object_type);
	if (marker == NULL)
	{
		pfree(upper_sql);
		return false;
	}

	identity_start = sql + (marker - upper_sql) + strlen(object_type);
	while (*identity_start != '\0' && isspace((unsigned char) *identity_start))
		identity_start++;
	identity_end = identity_start;
	while (*identity_end != '\0')
	{
		if (*identity_end == '"')
			quoted = !quoted;
		else if (!quoted &&
				 (isspace((unsigned char) *identity_end) ||
				  *identity_end == '(' || *identity_end == ';'))
			break;
		identity_end++;
	}

	pfree(upper_sql);
	if (identity_end <= identity_start)
		return false;

	*identity_copy = pnstrdup(identity_start, identity_end - identity_start);
	return true;
}

static bool
oggit_rule_target_from_text(const char *text, char **target_copy)
{
	char	   *upper_text;
	char	   *marker;
	const char *target_start;
	const char *target_end;
	int			i;

	if (text == NULL || text[0] == '\0')
		return false;

	upper_text = pstrdup(text);
	for (i = 0; upper_text[i]; i++)
		upper_text[i] = pg_toupper((unsigned char) upper_text[i]);

	marker = strstr(upper_text, " TO ");
	if (marker != NULL)
		target_start = text + (marker - upper_text) + 4;
	else
	{
		marker = strstr(upper_text, " ON ");
		if (marker == NULL)
		{
			pfree(upper_text);
			return false;
		}
		target_start = text + (marker - upper_text) + 4;
	}

	while (*target_start != '\0' && isspace((unsigned char) *target_start))
		target_start++;
	target_end = target_start;
	while (*target_end != '\0' &&
		   !isspace((unsigned char) *target_end) &&
		   *target_end != ';' &&
		   *target_end != ',' &&
		   *target_end != ')' &&
		   *target_end != '"')
		target_end++;

	pfree(upper_text);
	if (target_end <= target_start)
		return false;

	*target_copy = pnstrdup(target_start, target_end - target_start);
	return true;
}

/*
 * Build a JSON object string that collapses neon_oggit tuple payload
 * ({col: {value, is_key, type}}) into a flat {col: value} object, matching
 * the Rust oggit_tuple_payload_to_json(). Returns a palloc'd cJSON object
 * (caller cJSON_Delete) or NULL.
 */
static cJSON *
oggit_tuple_payload_to_flat(const cJSON *payload)
{
	cJSON	   *result;
	const cJSON *field;

	if (payload == NULL || !cJSON_IsObject(payload))
		return NULL;

	result = cJSON_CreateObject();
	cJSON_ArrayForEach(field, payload)
	{
		const cJSON *value = cJSON_GetObjectItemCaseSensitive(field, "value");

		if (value != NULL)
			cJSON_AddItemToObject(result, field->string,
								  cJSON_Duplicate(value, true));
	}
	return result;
}

static cJSON *
oggit_flat_project_cols(const cJSON *row, const cJSON *columns)
{
	cJSON	   *result;
	const cJSON *column;

	if (row == NULL || !cJSON_IsObject(row))
		return NULL;

	result = cJSON_CreateObject();
	if (columns == NULL || !cJSON_IsArray(columns))
		return result;

	cJSON_ArrayForEach(column, columns)
	{
		const cJSON *value;

		if (!cJSON_IsString(column) || column->valuestring == NULL)
			continue;
		value = cJSON_GetObjectItemCaseSensitive(row, column->valuestring);
		if (value != NULL)
			cJSON_AddItemToObject(result, column->valuestring,
								  cJSON_Duplicate(value, true));
	}
	return result;
}

/* Collect non-key column names from a tuple payload (sorted like Rust). */
static cJSON *
oggit_tuple_nonkey_cols(const cJSON *payload)
{
	cJSON	   *arr = cJSON_CreateArray();
	const cJSON *field;

	if (payload != NULL && cJSON_IsObject(payload))
	{
		cJSON_ArrayForEach(field, payload)
		{
			const cJSON *is_key = cJSON_GetObjectItemCaseSensitive(field, "is_key");

			if (!(is_key != NULL && cJSON_IsBool(is_key) && cJSON_IsTrue(is_key)))
				cJSON_AddItemToArray(arr, cJSON_CreateString(field->string));
		}
	}
	return arr;
}

/*
 * SPI helper: execute an INSERT with text/jsonb args. All arguments are
 * passed as text and cast in SQL, matching how the Rust worker bound them.
 */
static void
oggit_spi_exec_args(const char *sql, int nargs, Oid *argtypes,
					Datum *values, const char *nulls)
{
	int			rc = SPI_execute_with_args(sql, nargs, argtypes, values, nulls,
										   false, 0, NULL);

	if (rc < 0)
		elog(ERROR, "oggit: SPI_execute_with_args failed (%d): %s", rc, sql);
}

static bool
oggit_relation_has_primary_key(const char *schema, const char *table)
{
	Oid			argtypes[2];
	Datum		values[2];
	char		nulls[2] = {' ', ' '};
	bool		has_primary = false;
	bool		isnull = false;
	Datum		value;
	int			rc;

	if (schema == NULL || table == NULL)
		return false;

	argtypes[0] = TEXTOID;
	argtypes[1] = TEXTOID;
	values[0] = CStringGetTextDatum(schema);
	values[1] = CStringGetTextDatum(table);

	rc = SPI_execute_with_args(
							   "SELECT EXISTS ("
							   " SELECT 1"
							   "   FROM pg_class c"
							   "   JOIN pg_namespace n ON n.oid = c.relnamespace"
							   "   JOIN pg_index i ON i.indrelid = c.oid"
							   "  WHERE n.nspname = $1"
							   "    AND c.relname = $2"
							   "    AND i.indisprimary)",
							   2, argtypes, values, nulls, true, 1, NULL);
	if (rc != SPI_OK_SELECT || SPI_processed == 0)
		return false;

	value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (!isnull)
		has_primary = DatumGetBool(value);
	return has_primary;
}

static void
oggit_update_worker_status(const char *status, const char *last_error)
{
	Oid			argtypes[2];
	Datum		values[2];
	char		nulls[2] = {' ', ' '};

	PG_TRY();
	{
		if (oggit_state_table_exists())
		{
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			if (SPI_connect() != SPI_OK_CONNECT)
				elog(ERROR, "oggit: SPI_connect failed while updating worker status");

			argtypes[0] = TEXTOID;
			argtypes[1] = TEXTOID;
			values[0] = CStringGetTextDatum(status);
			if (last_error == NULL || last_error[0] == '\0')
			{
				values[1] = (Datum) 0;
				nulls[1] = 'n';
			}
			else
				values[1] = CStringGetTextDatum(last_error);

			oggit_spi_exec_args("UPDATE oggit.state "
								"SET status = $1, last_error = $2, updated_at = now() WHERE id = true",
								2, argtypes, values, nulls);

			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
	}
	PG_CATCH();
	{
		FlushErrorState();
		AbortOutOfAnyTransaction();
	}
	PG_END_TRY();
}

static void
oggit_update_required_lsn(XLogRecPtr required_lsn)
{
	char	   *required_lsn_str;
	Oid			argtypes[1];
	Datum		values[1];
	char		nulls[1] = {' '};

	if (XLByteEQ(required_lsn, InvalidXLogRecPtr))
		return;

	required_lsn_str = oggit_lsn_to_string(required_lsn);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "oggit: SPI_connect failed while updating required_lsn");

	argtypes[0] = TEXTOID;
	values[0] = CStringGetTextDatum(required_lsn_str);
	oggit_spi_exec_args("UPDATE oggit.state "
						"SET required_lsn = $1, updated_at = now() WHERE id = true",
						1, argtypes, values, nulls);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	oggit_report_required_lsn_to_safekeepers(required_lsn_str);
	pfree(required_lsn_str);
}

static Datum
oggit_text_or_null(const char *s, char *nullflag)
{
	if (s == NULL)
	{
		*nullflag = 'n';
		return (Datum) 0;
	}
	*nullflag = ' ';
	return CStringGetTextDatum(s);
}

/*
 * Persist a single decoded neon_oggit event. Mirrors the semantics of the
 * former Rust oggit_record_event(): "change" -> oggit.change_log,
 * "truncate"/"ddl"/other -> oggit.object_change, "commit" -> advance state.
 */
static void
oggit_record_event_json(const char *json, int ordinal)
{
	cJSON	   *event = cJSON_Parse(json);
	const char *event_kind;
	const char *commit_lsn;

	if (event == NULL)
	{
		elog(WARNING, "oggit: failed to parse neon_oggit event json");
		return;
	}

	event_kind = oggit_json_str(event, "event");
	commit_lsn = oggit_event_lsn(event);

	if (event_kind != NULL && strcmp(event_kind, "change") == 0)
	{
		const char *schema = oggit_json_str(event, "schema");
		const char *table = oggit_json_str(event, "table");
		const char *op = oggit_json_str(event, "op");
		cJSON	   *key_flat;
		cJSON	   *old_flat;
		cJSON	   *new_flat;
		cJSON	   *changed_cols;
		const char *identity;
		const char *event_identity;
		char	   *key_text = NULL;
		char	   *old_text = NULL;
		char	   *new_text = NULL;
		char	   *changed_text = NULL;
		const char *unsupported_reason = NULL;
		const char *relid = oggit_json_str(event, "relid");
		const char *xid = oggit_json_str(event, "xid");
		const char *merge_id = oggit_json_str(event, "merge_id");
		const char *record_lsn = oggit_json_str(event, "change_lsn");
		StringInfoData sql;
		Oid			argtypes[15];
		Datum		values[15];
		char		nulls[15];
		int			i;

		if (oggit_is_internal_schema(schema))
		{
			cJSON_Delete(event);
			return;
		}

		key_flat = oggit_tuple_payload_to_flat(cJSON_GetObjectItemCaseSensitive(event, "key"));
		old_flat = oggit_tuple_payload_to_flat(cJSON_GetObjectItemCaseSensitive(event, "old_row"));
		new_flat = oggit_tuple_payload_to_flat(cJSON_GetObjectItemCaseSensitive(event, "new_row"));

		/* Prefer the output plugin's relation identity; infer only for legacy events. */
		event_identity = oggit_json_str(event, "identity_kind");
		if (event_identity != NULL &&
			(strcmp(event_identity, "primary_key") == 0 ||
			 strcmp(event_identity, "unique_key") == 0 ||
			 strcmp(event_identity, "replica_identity_full") == 0 ||
			 strcmp(event_identity, "unsupported") == 0))
			identity = event_identity;
		else if (key_flat != NULL && cJSON_GetArraySize(key_flat) > 0)
			identity = oggit_relation_has_primary_key(schema, table) ? "primary_key" : "unique_key";
		else if (old_flat != NULL || new_flat != NULL)
			identity = "replica_identity_full";
		else
			identity = "unsupported";

		if (strcmp(identity, "unsupported") == 0)
			unsupported_reason = "no usable row identity in decoded event";

		/* changed_cols: for INSERT, non-key columns of new_row; else event.changed_cols. */
		if (op != NULL && strcmp(op, "INSERT") == 0)
			changed_cols = oggit_tuple_nonkey_cols(cJSON_GetObjectItemCaseSensitive(event, "new_row"));
		else
		{
			cJSON	   *src = cJSON_GetObjectItemCaseSensitive(event, "changed_cols");

			changed_cols = (src != NULL) ? cJSON_Duplicate(src, true) : cJSON_CreateArray();
		}

		/*
		 * Keyed UPDATEs store only changed columns. FULL identity must retain the
		 * complete before/after tuples because the old row is the row locator.
		 */
		if (op != NULL && strcmp(op, "UPDATE") == 0 &&
			strcmp(identity, "replica_identity_full") != 0)
		{
			cJSON	   *projected_old = oggit_flat_project_cols(old_flat, changed_cols);
			cJSON	   *projected_new = oggit_flat_project_cols(new_flat, changed_cols);

			if (old_flat)
				cJSON_Delete(old_flat);
			if (new_flat)
				cJSON_Delete(new_flat);
			old_flat = projected_old;
			new_flat = projected_new;
		}

		/* key_json is NULL when empty. */
		if (key_flat != NULL && cJSON_GetArraySize(key_flat) > 0)
			key_text = cJSON_PrintUnformatted(key_flat);
		if (old_flat != NULL && cJSON_GetArraySize(old_flat) > 0)
			old_text = cJSON_PrintUnformatted(old_flat);
		if (new_flat != NULL && cJSON_GetArraySize(new_flat) > 0)
			new_text = cJSON_PrintUnformatted(new_flat);
		changed_text = cJSON_PrintUnformatted(changed_cols);

		initStringInfo(&sql);
		appendStringInfoString(&sql,
							   "INSERT INTO oggit.change_log ("
							   " commit_lsn, record_lsn, xid, merge_id, ordinal, op, schema_name, table_name,"
							   " relid, identity_kind, key_json, old_row, new_row, changed_cols,"
							   " unsupported_reason) SELECT "
							   " $1, $2, $3, $4::uuid, $5, $6, $7, $8, $9, $10,"
							   " $11::jsonb, $12::jsonb, $13::jsonb,"
							   " (SELECT COALESCE(array_agg(value::text), ARRAY[]::text[])"
							   "    FROM json_array_elements_text($14::json)), $15"
							   " WHERE NOT EXISTS ("
							   "   SELECT 1 FROM oggit.change_log"
							   "    WHERE commit_lsn = $1"
							   "      AND ordinal = $5"
							   "      AND op = $6"
							   "      AND COALESCE(schema_name, '') = COALESCE($7, '')"
							   "      AND COALESCE(table_name, '') = COALESCE($8, '')"
							   "      AND COALESCE(key_json::text, '') = COALESCE(($11::jsonb)::text, '')"
							   "      AND COALESCE(old_row::text, '') = COALESCE(($12::jsonb)::text, '')"
							   "      AND COALESCE(new_row::text, '') = COALESCE(($13::jsonb)::text, '')"
							   " )");

		for (i = 0; i < 15; i++)
			argtypes[i] = TEXTOID;
		argtypes[4] = INT4OID;	/* ordinal */
		argtypes[8] = OIDOID;	/* relid */

		values[0] = CStringGetTextDatum(commit_lsn);
		nulls[0] = ' ';
		values[1] = oggit_text_or_null(record_lsn, &nulls[1]);
		values[2] = oggit_text_or_null(xid, &nulls[2]);
		values[3] = oggit_text_or_null(merge_id, &nulls[3]);
		values[4] = Int32GetDatum(ordinal);
		nulls[4] = ' ';
		values[5] = CStringGetTextDatum(op != NULL ? op : "UNSUPPORTED");
		nulls[5] = ' ';
		values[6] = oggit_text_or_null(schema, &nulls[6]);
		values[7] = oggit_text_or_null(table, &nulls[7]);
		if (relid != NULL && relid[0] != '\0')
		{
			values[8] = ObjectIdGetDatum((Oid) strtoul(relid, NULL, 10));
			nulls[8] = ' ';
		}
		else
		{
			values[8] = (Datum) 0;
			nulls[8] = 'n';
		}
		values[9] = CStringGetTextDatum(identity);
		nulls[9] = ' ';
		values[10] = oggit_text_or_null(key_text, &nulls[10]);
		values[11] = oggit_text_or_null(old_text, &nulls[11]);
		values[12] = oggit_text_or_null(new_text, &nulls[12]);
		values[13] = CStringGetTextDatum(changed_text);
		nulls[13] = ' ';
		values[14] = oggit_text_or_null(unsupported_reason, &nulls[14]);

		oggit_spi_exec_args(sql.data, 15, argtypes, values, nulls);

		pfree(sql.data);
		if (key_text)
			cJSON_free(key_text);
		if (old_text)
			cJSON_free(old_text);
		if (new_text)
			cJSON_free(new_text);
		if (changed_text)
			cJSON_free(changed_text);
		if (key_flat)
			cJSON_Delete(key_flat);
		if (old_flat)
			cJSON_Delete(old_flat);
		if (new_flat)
			cJSON_Delete(new_flat);
		cJSON_Delete(changed_cols);
	}
	else if (event_kind != NULL && strcmp(event_kind, "truncate") == 0)
	{
		const char *rel_schema = NULL;
		const char *rel_table = NULL;
		const char *merge_id = oggit_json_str(event, "merge_id");
		char	   *replay_sql = oggit_truncate_replay_sql(event, &rel_schema, &rel_table);
		char	   *change_json = NULL;
		StringInfoData sql;
		Oid			argtypes[6];
		Datum		values[6];
		char		nulls[6];

		if (replay_sql == NULL)
		{
			cJSON_Delete(event);
			return;
		}
		const char *existing_sql = oggit_json_str(event, "sql");

		if (existing_sql == NULL)
			cJSON_AddStringToObject(event, "sql", replay_sql);
		else if (existing_sql[0] == '\0')
			cJSON_ReplaceItemInObjectCaseSensitive(event, "sql",
												   cJSON_CreateString(replay_sql));
		if (cJSON_GetObjectItemCaseSensitive(event, "cascade") == NULL)
			cJSON_AddFalseToObject(event, "cascade");
		if (cJSON_GetObjectItemCaseSensitive(event, "restart_seqs") == NULL)
			cJSON_AddFalseToObject(event, "restart_seqs");
		change_json = cJSON_PrintUnformatted(event);

		initStringInfo(&sql);
		appendStringInfoString(&sql,
							   "INSERT INTO oggit.object_change ("
							   " commit_lsn, merge_id, ordinal, object_type, schema_name, object_name,"
							   " action, change_json, safety_class) SELECT "
							   " $1, $2::uuid, $3, 'TRUNCATE', $4, $5, 'TRUNCATE', $6::jsonb, 'destructive'"
							   " WHERE NOT EXISTS ("
							   "   SELECT 1 FROM oggit.object_change"
							   "    WHERE commit_lsn = $1"
							   "      AND ordinal = $3"
							   "      AND action = 'TRUNCATE'"
							   "      AND COALESCE(change_json::text, '') = COALESCE(($6::jsonb)::text, '')"
							   " )");
		argtypes[0] = TEXTOID;
		argtypes[1] = TEXTOID;
		argtypes[2] = INT4OID;
		argtypes[3] = TEXTOID;
		argtypes[4] = TEXTOID;
		argtypes[5] = TEXTOID;
		values[0] = CStringGetTextDatum(commit_lsn);
		nulls[0] = ' ';
		values[1] = oggit_text_or_null(merge_id, &nulls[1]);
		values[2] = Int32GetDatum(ordinal);
		nulls[2] = ' ';
		values[3] = oggit_text_or_null(rel_schema, &nulls[3]);
		values[4] = oggit_text_or_null(rel_table, &nulls[4]);
		values[5] = CStringGetTextDatum(change_json);
		nulls[5] = ' ';

		oggit_spi_exec_args(sql.data, 6, argtypes, values, nulls);
		pfree(sql.data);
		pfree(replay_sql);
		if (change_json)
			cJSON_free(change_json);
	}
		else if (event_kind != NULL && strcmp(event_kind, "ddl") == 0)
		{
			 /*
			 * DDL classification mirrors the Rust worker: derive object_type and
			 * safety_class from cmdtype + message text.
			 */
			const char *message = oggit_json_str(event, "message");
			const char *cmdtype = oggit_json_str(event, "cmdtype");
			const char *merge_id = oggit_json_str(event, "merge_id");
			cJSON	   *ddl_payload = NULL;
			const char *raw_type = NULL;
			const char *object_type;
			const char *object_schema = NULL;
			const char *object_name = NULL;
			const char *objidentity = NULL;
			const char *safety_class;
			const char *unsupported_reason = NULL;
			char	   *message_copy = NULL;
			char	   *replay_sql = NULL;
			char	   *owner = NULL;
			char	   *change_json;
			char	   *upper_sql;
			char	   *upper_cmdtype;
			char	   *objidentity_copy = NULL;
			char	   *parsed_object_copy = NULL;
			int			mi;
			StringInfoData sql;
			Oid			argtypes[10];
			Datum		values[10];
			char		nulls[10];

			if (message == NULL)
				message = "";
			if (cmdtype == NULL)
				cmdtype = "DDL";

			if (message[0] != '\0')
			{
				message_copy = pstrdup(message);
				ddl_payload = cJSON_Parse(message);
				if (ddl_payload != NULL && cJSON_IsObject(ddl_payload))
				{
					replay_sql = oggit_deparse_ddl_json_to_string(message_copy, &owner);
					if (replay_sql != NULL && replay_sql[0] != '\0')
						cJSON_AddStringToObject(event, "sql", replay_sql);
				}
			}

			if (ddl_payload == NULL &&
				(strcmp(cmdtype, "table_drop_start") == 0 ||
				 strcmp(cmdtype, "type_drop_start") == 0))
			{
				if (message_copy)
					pfree(message_copy);
				cJSON_Delete(event);
				return;
			}

			if (ddl_payload != NULL && cJSON_IsObject(ddl_payload))
			{
				raw_type = oggit_json_str(ddl_payload, "objtype");
				object_schema = oggit_json_nested_str(ddl_payload, "identity", "schemaname");
				object_name = oggit_json_nested_str(ddl_payload, "identity", "objname");
				objidentity = oggit_json_str(ddl_payload, "objidentity");
				if (object_name == NULL && objidentity != NULL)
				{
					char	   *dot;

					objidentity_copy = pstrdup(objidentity);
					dot = strchr(objidentity_copy, '.');
					if (dot != NULL)
					{
						*dot = '\0';
						object_schema = objidentity_copy;
						object_name = dot + 1;
					}
					else
						object_name = objidentity_copy;
				}
				if (object_name == NULL)
					object_name = oggit_json_str(ddl_payload, "name");
			}

				if (oggit_is_internal_schema(object_schema) ||
					(object_schema == NULL && oggit_is_internal_schema(object_name)) ||
					oggit_json_mentions_internal_schema(ddl_payload) ||
					oggit_json_mentions_merge_barrier(ddl_payload) ||
					oggit_sql_mentions_internal_schema_ddl(replay_sql) ||
					oggit_sql_mentions_internal_schema_ddl(message) ||
					oggit_string_mentions_merge_barrier(replay_sql) ||
					oggit_string_mentions_merge_barrier(message))
				{
					if (ddl_payload)
						cJSON_Delete(ddl_payload);
					if (message_copy)
					pfree(message_copy);
				if (replay_sql)
					pfree(replay_sql);
				if (owner)
					pfree(owner);
				if (objidentity_copy)
					pfree(objidentity_copy);
				if (parsed_object_copy)
					pfree(parsed_object_copy);
				cJSON_Delete(event);
				return;
			}

			upper_sql = pstrdup((replay_sql != NULL && replay_sql[0] != '\0') ? replay_sql : message);
			for (mi = 0; upper_sql[mi]; mi++)
				upper_sql[mi] = pg_toupper((unsigned char) upper_sql[mi]);

			upper_cmdtype = pstrdup(cmdtype);
			for (mi = 0; upper_cmdtype[mi]; mi++)
				upper_cmdtype[mi] = pg_toupper((unsigned char) upper_cmdtype[mi]);

			object_type = oggit_ddl_object_type(raw_type, upper_sql);
			if (strcmp(object_type, "TRIGGER") == 0 &&
				object_schema == NULL && ddl_payload != NULL)
				object_schema = oggit_json_nested_str(ddl_payload, "relation", "schemaname");
			if (object_name == NULL &&
				(strcmp(object_type, "TRIGGER") == 0 ||
				 strcmp(object_type, "FUNCTION") == 0 ||
				 strcmp(object_type, "VIEW") == 0))
			{
				char	   *dot;

				if (oggit_ddl_identity_from_sql(replay_sql, object_type,
											&parsed_object_copy))
				{
					dot = strrchr(parsed_object_copy, '.');
					if (dot != NULL)
					{
						*dot = '\0';
						object_schema = parsed_object_copy;
						object_name = dot + 1;
					}
					else
						object_name = parsed_object_copy;
				}
			}
			if (strcmp(object_type, "COLUMN") == 0 && ddl_payload != NULL)
			{
				const char *column_name = oggit_json_str(ddl_payload, "colname");

				if (object_name == NULL && column_name != NULL)
					object_name = column_name;
			}
			if (strcmp(object_type, "RULE") == 0)
			{
				char	   *rule_target = NULL;

				if ((replay_sql != NULL &&
					 oggit_rule_target_from_text(replay_sql, &rule_target)) ||
					oggit_rule_target_from_text(message, &rule_target) ||
					oggit_rule_target_from_text(objidentity, &rule_target))
				{
					char	   *dot;

					parsed_object_copy = rule_target;
					dot = strrchr(parsed_object_copy, '.');
					if (dot != NULL)
					{
						*dot = '\0';
						object_schema = parsed_object_copy;
						object_name = dot + 1;
					}
					else
						object_name = parsed_object_copy;
				}
			}

			if (strstr(upper_cmdtype, "DROP") != NULL ||
				strstr(upper_sql, "DROP ") != NULL)
				safety_class = "destructive";
			else if ((strcmp(cmdtype, "table_alter") == 0 ||
					  strstr(upper_sql, "ALTER TABLE") != NULL) &&
					 (strstr(upper_sql, "ADD CONSTRAINT") != NULL ||
					  strstr(upper_sql, "CHECK") != NULL ||
					  strstr(upper_sql, "FOREIGN KEY") != NULL ||
					  strstr(upper_sql, "SET NOT NULL") != NULL))
				safety_class = "requires_validation";
			else if ((strcmp(cmdtype, "table_alter") == 0 ||
					  strstr(upper_sql, "ALTER TABLE") != NULL) &&
					 (strstr(upper_sql, "RENAME COLUMN") != NULL ||
					  strstr(upper_sql, " RENAME TO ") != NULL ||
					  (strstr(upper_sql, "ALTER COLUMN") != NULL &&
					   (strstr(upper_sql, " TYPE ") != NULL ||
						strstr(upper_sql, "SET DATA TYPE") != NULL))))
				safety_class = "semantic";
			else if (((strcmp(object_type, "COLUMN") == 0 ||
					   (strcmp(object_type, "TABLE") == 0 &&
						strstr(upper_sql, "ADD COLUMN") != NULL)) &&
					  strcmp(cmdtype, "table_alter") == 0 &&
					  strstr(upper_sql, "NOT NULL") == NULL &&
					  strstr(upper_sql, "UNIQUE") == NULL &&
					  strstr(upper_sql, "CHECK") == NULL &&
					  strstr(upper_sql, "FOREIGN KEY") == NULL) ||
					 (strcmp(object_type, "INDEX") == 0 &&
					  strstr(upper_sql, "CREATE") != NULL &&
					  strstr(upper_sql, "INDEX") != NULL &&
					  strstr(upper_sql, "UNIQUE") == NULL) ||
					 (strstr(upper_sql, "COMMENT ON ") != NULL) ||
					 (strcmp(object_type, "TABLE") == 0 &&
					  strstr(upper_sql, "CREATE") != NULL &&
					  strstr(upper_sql, "TABLE") != NULL) ||
					 (strcmp(object_type, "SCHEMA") == 0 &&
					  strstr(upper_sql, "CREATE") != NULL &&
					  strstr(upper_sql, "SCHEMA") != NULL))
				safety_class = "safe_additive";
			else if (strcmp(object_type, "SEQUENCE") == 0 ||
					 strcmp(object_type, "TRIGGER") == 0 ||
					 strcmp(object_type, "FUNCTION") == 0 ||
					 strcmp(object_type, "VIEW") == 0)
				safety_class = "semantic";
			else if (strcmp(object_type, "CONSTRAINT") == 0)
				safety_class = "requires_validation";
			else
				safety_class = "unsupported";

			if (strcmp(safety_class, "unsupported") == 0)
				unsupported_reason = "DDL is not supported by object-level merge";

			change_json = cJSON_PrintUnformatted(event);

			initStringInfo(&sql);
			appendStringInfoString(&sql,
								   "INSERT INTO oggit.object_change ("
								   " commit_lsn, merge_id, ordinal, object_type, schema_name, object_name,"
								   " action, change_json, safety_class, unsupported_reason) SELECT "
								   " $1, $2::uuid, $3, $4, $5, $6, $7, $8::jsonb, $9, $10"
								   " WHERE NOT EXISTS ("
								   "   SELECT 1 FROM oggit.object_change"
								   "    WHERE commit_lsn = $1"
								   "      AND ordinal = $3"
								   "      AND action = $7"
								   "      AND COALESCE(change_json::text, '') = COALESCE(($8::jsonb)::text, '')"
								   " )");
			for (mi = 0; mi < 10; mi++)
				argtypes[mi] = TEXTOID;
			argtypes[2] = INT4OID;
			values[0] = CStringGetTextDatum(commit_lsn);
			nulls[0] = ' ';
			values[1] = oggit_text_or_null(merge_id, &nulls[1]);
			values[2] = Int32GetDatum(ordinal);
			nulls[2] = ' ';
			values[3] = CStringGetTextDatum(object_type);
			nulls[3] = ' ';
			values[4] = oggit_text_or_null(object_schema, &nulls[4]);
			values[5] = oggit_text_or_null(object_name, &nulls[5]);
			values[6] = CStringGetTextDatum(cmdtype);
			nulls[6] = ' ';
			values[7] = CStringGetTextDatum(change_json);
			nulls[7] = ' ';
			values[8] = CStringGetTextDatum(safety_class);
			nulls[8] = ' ';
			values[9] = oggit_text_or_null(unsupported_reason, &nulls[9]);

			oggit_spi_exec_args(sql.data, 10, argtypes, values, nulls);
			pfree(sql.data);
			pfree(upper_sql);
			pfree(upper_cmdtype);
			if (ddl_payload)
				cJSON_Delete(ddl_payload);
			if (message_copy)
				pfree(message_copy);
			if (replay_sql)
				pfree(replay_sql);
			if (owner)
				pfree(owner);
			if (objidentity_copy)
				pfree(objidentity_copy);
			if (parsed_object_copy)
				pfree(parsed_object_copy);
			if (change_json)
				cJSON_free(change_json);
		}
	else if (event_kind != NULL && strcmp(event_kind, "commit") == 0)
	{
		const char *confirmed = oggit_json_str(event, "callback_commit_lsn");

		if (confirmed == NULL)
			confirmed = commit_lsn;

		/*
		 * Defer decode_lsn/confirmed_lsn updates to oggit_persist_batch().
		 * Per-commit UPDATEs against oggit.state were a hot path under TPC-C
		 * and interacted badly with anomalous duplicate id=true rows.
		 */
		oggit_note_batch_commit_lsn(commit_lsn, confirmed);
	}
		else
		{
			/* Unknown event kind: record as unsupported object_change. */
			const char *merge_id = oggit_json_str(event, "merge_id");
			char	   *change_json = cJSON_PrintUnformatted(event);
			StringInfoData sql;
			Oid			argtypes[5];
			Datum		values[5];
			char		nulls[5];

			if (oggit_json_mentions_internal_schema(event) ||
				oggit_json_mentions_merge_barrier(event))
			{
				if (change_json)
					cJSON_free(change_json);
				cJSON_Delete(event);
				return;
			}

			initStringInfo(&sql);
			appendStringInfoString(&sql,
								   "INSERT INTO oggit.object_change ("
								   " commit_lsn, merge_id, ordinal, object_type, action, change_json,"
								   " safety_class, unsupported_reason) SELECT "
								   " $1, $2::uuid, $3, 'OTHER', $4, $5::jsonb, 'unsupported',"
								   " 'unknown neon_oggit event'"
								   " WHERE NOT EXISTS ("
								   "   SELECT 1 FROM oggit.object_change"
								   "    WHERE commit_lsn = $1"
								   "      AND ordinal = $3"
								   "      AND action = $4"
								   "      AND COALESCE(change_json::text, '') = COALESCE(($5::jsonb)::text, '')"
								   " )");
			argtypes[0] = TEXTOID;
			argtypes[1] = TEXTOID;
			argtypes[2] = INT4OID;
			argtypes[3] = TEXTOID;
			argtypes[4] = TEXTOID;
			values[0] = CStringGetTextDatum(commit_lsn);
			nulls[0] = ' ';
			values[1] = oggit_text_or_null(merge_id, &nulls[1]);
			values[2] = Int32GetDatum(ordinal);
			nulls[2] = ' ';
			values[3] = CStringGetTextDatum(event_kind != NULL ? event_kind : "unknown");
			nulls[3] = ' ';
			values[4] = CStringGetTextDatum(change_json);
			nulls[4] = ' ';
			oggit_spi_exec_args(sql.data, 5, argtypes, values, nulls);
		pfree(sql.data);
		if (change_json)
			cJSON_free(change_json);
	}

	cJSON_Delete(event);
}

/* ------------------------------------------------------------------------
 * Worker main
 * ------------------------------------------------------------------------ */

/*
 * Bootstrap: ensure the logical slot, then seed metadata. Slot creation must
 * run in a transaction that has not performed writes.
 */
static void
oggit_bootstrap(void)
{
	elog(LOG, "oggit worker: bootstrap begin");

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "oggit: SPI_connect failed during bootstrap");

	elog(LOG, "oggit worker: ensure slot");
	oggit_ensure_slot();

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "oggit: SPI_connect failed during bootstrap");

	elog(LOG, "oggit worker: bootstrap state");
	oggit_bootstrap_state();

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	elog(LOG, "oggit worker: bootstrap done");
}

/*
 * Persist all buffered events of the current batch through SPI, then advance
 * the slot to end_lsn. Runs in its own transaction, after the decoding
 * context has been torn down (so no historic catalog snapshot is active).
 */
static int
oggit_persist_batch(XLogRecPtr scanned_lsn, bool stopped_by_limit,
					bool stopped_by_shutdown)
{
	ListCell   *lc;
	int			ordinal = 0;
	int			nevents = list_length(oggit_event_buf);
	uint64		payload_bytes = oggit_batch_payload_bytes;
	char		previous_commit_lsn[64] = "";
	char	   *scanned_lsn_str;
	Oid			argtypes[1];
	Datum		values[1];
	char		nulls[1] = {' '};

	if (XLByteEQ(scanned_lsn, InvalidXLogRecPtr))
		return nevents;

	scanned_lsn_str = oggit_lsn_to_string(scanned_lsn);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "oggit: SPI_connect failed while persisting batch");

	foreach(lc, oggit_event_buf)
	{
		const char *json = (const char *) lfirst(lc);
		char		commit_lsn[64];

		oggit_event_commit_lsn(json, commit_lsn, sizeof(commit_lsn));
		if (strcmp(previous_commit_lsn, commit_lsn) != 0)
		{
			strlcpy(previous_commit_lsn, commit_lsn,
					sizeof(previous_commit_lsn));
			ordinal = 0;
		}
		ordinal++;
		oggit_record_event_json(json, ordinal);
	}

	if (!XLByteEQ(oggit_batch_max_decode_lsn, InvalidXLogRecPtr) ||
		!XLByteEQ(oggit_batch_max_confirmed_lsn, InvalidXLogRecPtr))
	{
		char	   *decode_lsn_str;
		char	   *confirmed_lsn_str;
		Oid			lsn_argtypes[3];
		Datum		lsn_values[3];
		char		lsn_nulls[3] = {' ', ' ', ' '};

		decode_lsn_str = oggit_lsn_to_string(
			XLByteEQ(oggit_batch_max_decode_lsn, InvalidXLogRecPtr)
				? scanned_lsn : oggit_batch_max_decode_lsn);
		confirmed_lsn_str = oggit_lsn_to_string(
			XLByteEQ(oggit_batch_max_confirmed_lsn, InvalidXLogRecPtr)
				? (XLByteEQ(oggit_batch_max_decode_lsn, InvalidXLogRecPtr)
					   ? scanned_lsn : oggit_batch_max_decode_lsn)
				: oggit_batch_max_confirmed_lsn);

		lsn_argtypes[0] = TEXTOID;
		lsn_argtypes[1] = TEXTOID;
		lsn_argtypes[2] = TEXTOID;
		lsn_values[0] = CStringGetTextDatum(scanned_lsn_str);
		lsn_values[1] = CStringGetTextDatum(decode_lsn_str);
		lsn_values[2] = CStringGetTextDatum(confirmed_lsn_str);
		oggit_spi_exec_args(
			"UPDATE oggit.state SET scanned_lsn = $1, decode_lsn = $2, "
			"confirmed_lsn = $3, updated_at = now() WHERE id = true",
			3, lsn_argtypes, lsn_values, lsn_nulls);
		pfree(decode_lsn_str);
		pfree(confirmed_lsn_str);
	}
	else
	{
		argtypes[0] = TEXTOID;
		values[0] = CStringGetTextDatum(scanned_lsn_str);
		oggit_spi_exec_args(
			"UPDATE oggit.state SET scanned_lsn = $1, updated_at = now() WHERE id = true",
			1, argtypes, values, nulls);
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	if (nevents > 0 || stopped_by_limit || stopped_by_shutdown)
		elog(LOG,
			 "oggit batch persisted: persisted_events=%d payload_bytes=" UINT64_FORMAT
			 " scanned_lsn=%s stopped_by_limit=%s stopped_by_shutdown=%s",
			 nevents, payload_bytes, scanned_lsn_str,
			 stopped_by_limit ? "true" : "false",
			 stopped_by_shutdown ? "true" : "false");

	pfree(scanned_lsn_str);

	/* Free the buffered JSON strings and reset batch accounting. */
	oggit_reset_batch();

	return nevents;
}

static void
oggit_start_decode_session(OggitDecodeSession *session)
{
	LogicalDecodingContext *ctx;
	MemoryContext old_context;

	Assert(session->ctx == NULL);
	Assert(t_thrd.slot_cxt.MyReplicationSlot == NULL);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	CheckLogicalDecodingRequirements(u_sess->proc_cxt.MyDatabaseId);
	ReplicationSlotAcquire(oggit_effective_slot_name, false);

	/*
	 * ReorderBuffer spill files are written under the slot's snap directory.
	 * Rebuild it only when the logical decoding session starts. Rebuilding it
	 * per batch would delete spill files for still-uncommitted transactions.
	 */
	LogicalCleanSnapDirectory(true);

	old_context = MemoryContextSwitchTo(oggit_decode_cxt);
	ctx = CreateDecodingContext(InvalidXLogRecPtr, NIL, false,
								logical_read_local_xlog_page,
								oggit_prepare_write, oggit_do_write);
	MemoryContextSwitchTo(old_context);

	session->ctx = ctx;
	session->startptr = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;

	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void
oggit_stop_decode_session(OggitDecodeSession *session)
{
	if (session->ctx != NULL)
	{
		FreeDecodingContext(session->ctx);
		session->ctx = NULL;
	}

	session->startptr = InvalidXLogRecPtr;

	if (t_thrd.slot_cxt.MyReplicationSlot != NULL)
		CleanMyReplicationSlot();

	if (oggit_decode_cxt != NULL)
		MemoryContextReset(oggit_decode_cxt);
}

/*
 * Decode all WAL currently available (up to the commit point captured at
 * entry) in one batch using the caller's long-lived logical decoding context,
 * then persist buffered events and advance the slot. Returns the number of
 * logical events persisted so the caller can decide whether to wait for more
 * WAL.
 *
 * The decoding context's page_read is replaced by NeonWALPageRead (via
 * Custom_XLogReaderRoutines) when safekeepers are configured, so
 * XLogReadRecord fetches WAL from the safekeeper and blocks on its socket.
 */
static int64
oggit_decode_one_batch(OggitDecodeSession *session)
{
	LogicalDecodingContext *ctx = session->ctx;
	XLogRecPtr	startptr = session->startptr;
	XLogRecPtr	end_of_wal;
	XLogRecPtr	last_end = InvalidXLogRecPtr;
	XLogRecPtr	scanned_lsn;
	ResourceOwner old_resowner = t_thrd.utils_cxt.CurrentResourceOwner;
	ResourceOwner decode_resowner;
	bool		stopped_by_limit = false;
	bool		stopped_by_shutdown = false;
	int			nevents = 0;

	Assert(ctx != NULL);
	Assert(t_thrd.slot_cxt.MyReplicationSlot != NULL);

	/*
	 * Safekeeper commit_lsn is the authoritative visibility boundary. A local
	 * flush/replay position can include WAL that has not reached quorum yet.
	 */
	if (!oggit_get_committed_decode_lsn(&end_of_wal))
	{
		elog(WARNING, "oggit worker: no safekeeper commit_lsn available; decode is paused");
		return 0;
	}

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	decode_resowner = ResourceOwnerCreate(old_resowner,
								 "oggit logical decoding",
								 THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
	t_thrd.utils_cxt.CurrentResourceOwner = decode_resowner;
	InvalidateSystemCaches();

	PG_TRY();
	{
		while ((!XLByteEQ(startptr, InvalidXLogRecPtr) && XLByteLT(startptr, end_of_wal)) ||
			   (!XLByteEQ(ctx->reader->EndRecPtr, InvalidXLogRecPtr) &&
				XLByteLT(ctx->reader->EndRecPtr, end_of_wal)))
		{
			XLogRecord *record;
			char	   *errm = NULL;

			record = XLogReadRecord(ctx->reader, startptr, &errm, true, SS_XLOGDIR);
			startptr = InvalidXLogRecPtr;

			if (errm != NULL)
				elog(ERROR, "oggit worker: could not read WAL at %X/%X: %s",
					 LSN_FORMAT_ARGS(ctx->reader->EndRecPtr), errm);

			if (record != NULL)
			{
				LogicalDecodingProcessRecord(ctx, ctx->reader);
				last_end = ctx->reader->EndRecPtr;

				/* Never split a WAL record or perform SPI from an output callback. */
				if (oggit_shutdown_requested)
				{
					stopped_by_shutdown = true;
					break;
				}
				if (oggit_batch_limit_reached())
				{
					stopped_by_limit = true;
					break;
				}
			}

			session->startptr = InvalidXLogRecPtr;

			if (oggit_shutdown_requested)
			{
				stopped_by_shutdown = true;
				break;
			}
			CHECK_FOR_INTERRUPTS();
		}
	}
	PG_CATCH();
	{
		oggit_release_resource_owner(decode_resowner, old_resowner, false);
		InvalidateSystemCaches();
		PG_RE_THROW();
	}
	PG_END_TRY();

	session->startptr = InvalidXLogRecPtr;

	oggit_release_resource_owner(decode_resowner, old_resowner, true);
	InvalidateSystemCaches();

	PopActiveSnapshot();
	CommitTransactionCommand();

	/*
	 * An early stop may only advance to a complete record actually processed by
	 * this batch. If no such record exists, do not persist or confirm progress.
	 */
	if (stopped_by_limit || stopped_by_shutdown)
	{
		if (XLByteEQ(last_end, InvalidXLogRecPtr))
			return 0;

		scanned_lsn = XLByteLT(last_end, end_of_wal) ? last_end : end_of_wal;
	}
	else
		scanned_lsn = end_of_wal;

	nevents = oggit_persist_batch(scanned_lsn, stopped_by_limit,
								stopped_by_shutdown);

	/* Advance the slot confirmed/restart position in its own transaction. */
	if (!XLByteEQ(last_end, InvalidXLogRecPtr))
	{
		XLogRecPtr	required_lsn;

		StartTransactionCommand();
		LogicalConfirmReceivedLocation(last_end);
		required_lsn = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;
		CommitTransactionCommand();

		oggit_update_required_lsn(required_lsn);
	}

	return nevents;
}

/*
 * Outer decode loop: repeatedly drain available WAL. When a batch decodes
 * nothing new, wait on the latch (with a bounded timeout) before retrying, so
 * the worker is effectively event-driven and does not busy-poll.
 */
static void
oggit_run_decode_loop(void)
{
	OggitDecodeSession session;

	MemSet(&session, 0, sizeof(session));
	session.startptr = InvalidXLogRecPtr;

	PG_TRY();
	{
		oggit_start_decode_session(&session);
		g_instance.pid_cxt.OggitWorkerReady = true;
		elog(LOG, "oggit worker: decode loop started on slot %s",
			 oggit_effective_slot_name);

		while (!oggit_shutdown_requested)
		{
			int64		decoded;

			if (oggit_got_sighup)
			{
				oggit_got_sighup = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			decoded = oggit_decode_one_batch(&session);

			if (oggit_shutdown_requested)
				break;

			if (decoded == 0)
			{
				(void) WaitLatch(&t_thrd.proc->procLatch,
								 WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
								 OGGIT_IDLE_NAP_MS);
				ResetLatch(&t_thrd.proc->procLatch);
			}

			CHECK_FOR_INTERRUPTS();
		}
	}
	PG_CATCH();
	{
		oggit_stop_decode_session(&session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	oggit_stop_decode_session(&session);
}

void
OggitWorkerMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;
	const char *dbname;

	/*
	 * Gating: the postmaster starts this thread whenever the neon plugin is
	 * loaded (mirroring WALPROPOSER). Honour the neon.oggit_enabled GUC and the
	 * required branch identity here; if disabled, exit quietly so the endpoint
	 * behaves as if no oggit worker exists.
	 */
	g_instance.pid_cxt.OggitWorkerReady = false;

	if (!oggit_enabled)
	{
		elog(LOG, "oggit worker: neon.oggit_enabled is off, exiting");
		proc_exit(0);
	}
	if (!oggit_guc_present(oggit_tenant_id) || !oggit_guc_present(oggit_timeline_id))
	{
		elog(LOG, "oggit worker: tenant/timeline GUCs empty, exiting");
		proc_exit(0);
	}

	t_thrd.role = OGGITWORKER;
	SetProcessingMode(InitProcessing);

	/* Signal handlers: react quickly to shutdown and reload. */
	gspqsignal(SIGTERM, oggit_shutdown_handler);
	gspqsignal(SIGHUP, oggit_sighup_handler);
	gspqsignal(SIGINT, StatementCancelHandler);
	gspqsignal(SIGQUIT, quickdie);
	gspqsignal(SIGALRM, handle_sig_alarm);
	gspqsignal(SIGPIPE, SIG_IGN);
	gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
	gspqsignal(SIGUSR2, oggit_shutdown_handler);
	gspqsignal(SIGFPE, FloatExceptionHandler);
	gspqsignal(SIGCHLD, SIG_DFL);

	/*
	 * The postmaster OGGITWORKER case already ran InitProcessAndShareMemory()
	 * (InitProcess + shared memory). BaseInit() is still needed before we can
	 * attach to a database.
	 */
	BaseInit();

	/* Standard error-recovery scope for a background thread. */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		AbortOutOfAnyTransaction();
		proc_exit(0);
	}
	t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;
	gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
	(void) gs_signal_unblock_sigusr2();

	/*
	 * Attach to the target database as cloud_admin. InitBgWorker() is the
	 * generic backend init path (InitThread -> InitSysCache -> StartXact ->
	 * InitUser -> SetDatabase -> LoadSysCache -> InitDatabase -> FinishInit),
	 * which is what enables SPI/executor + catalog access. It needs a valid
	 * MyProcPort->user_name (InitUser/CheckConnPermission), so set both the
	 * database and user names first. t_thrd.proc_cxt.PostInit is created per
	 * thread by knl_thread_init, so we just drive it here.
	 */
	dbname = oggit_guc_present(oggit_database) ? oggit_database : "postgres";
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

		if (u_sess->proc_cxt.MyProcPort->database_name)
			pfree_ext(u_sess->proc_cxt.MyProcPort->database_name);
		if (u_sess->proc_cxt.MyProcPort->user_name)
			pfree_ext(u_sess->proc_cxt.MyProcPort->user_name);
		u_sess->proc_cxt.MyProcPort->database_name = pstrdup(dbname);
		u_sess->proc_cxt.MyProcPort->user_name = pstrdup("cloud_admin");
		(void) MemoryContextSwitchTo(oldcxt);
	}
	u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();

	t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, "cloud_admin");
	t_thrd.proc_cxt.PostInit->InitBgWorker();
	t_thrd.proc_cxt.PostInit->GetDatabaseName(u_sess->proc_cxt.MyProcPort->database_name);
	oggit_initialize_effective_slot_name();

	/* Run as a normal backend from here on. */
	pgstat_report_activity(STATE_RUNNING, NULL);
	t_thrd.role = OGGITWORKER;

	/*
	 * A resource owner is required for buffer pins and SPI. InitBgWorker's
	 * FinishInit committed its bootstrap transaction, leaving none, so create
	 * one now (mirrors job_worker).
	 */
	t_thrd.utils_cxt.CurrentResourceOwner =
		ResourceOwnerCreate(NULL, "oggit worker",
							THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

	SetProcessingMode(NormalProcessing);
	pgstat_report_appname("OggitWorker");

	elog(LOG, "oggit worker started: tenant=%s timeline=%s slot=%s db=%s",
		 oggit_tenant_id, oggit_timeline_id, oggit_effective_slot_name, dbname);

	/*
	 * Long-lived context for buffering one decode batch's JSON events. It
	 * survives across the decode/persist transactions and is reset after each
	 * batch is persisted. TopMemoryContext is sealed in openGauss to catch
	 * leaks, so unseal it while creating our context (mirrors walproposer_pg).
	 */
	{
		bool		was_sealed = TopMemoryContext->is_sealed;

		if (was_sealed)
			MemoryContextUnSeal(TopMemoryContext);
		oggit_decode_cxt = AllocSetContextCreate(TopMemoryContext,
												 "oggit decode session",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);
		oggit_batch_cxt = AllocSetContextCreate(TopMemoryContext,
												"oggit event batch",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
		oggit_worker_cxt = AllocSetContextCreate(TopMemoryContext,
												 "oggit worker",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

		/*
		 * maskPassword() (invoked by the SPI error callback and by any DDL
		 * that creates plpgsql functions) switches to
		 * t_thrd.mem_cxt.mask_password_mem_cxt and palloc0's there. It is
		 * initialized to NULL per thread (knl_thread.cpp) and only set up by
		 * the standard backend/aux init paths, which this hand-rolled worker
		 * does not fully run. Create it here to avoid a NULL-context palloc
		 * crash during error unwinding / interrupt handling (mirrors
		 * job_worker.cpp).
		 */
		if (t_thrd.mem_cxt.mask_password_mem_cxt == NULL)
			t_thrd.mem_cxt.mask_password_mem_cxt =
				AllocSetContextCreate(t_thrd.top_mem_cxt,
									  "MaskPasswordCtx",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
		if (was_sealed)
			MemoryContextSeal(TopMemoryContext);
	}

	/*
	 * Bootstrap and decode are wrapped in a PG_TRY so that a transient error
	 * (e.g. WAL not yet available, catalog race at startup) marks the state
	 * failed and the worker naps before retrying instead of crashing the
	 * whole compute.
	 */
	while (!oggit_shutdown_requested)
	{
		g_instance.pid_cxt.OggitWorkerReady = false;
		PG_TRY();
		{
			if (!oggit_wait_for_state_table())
				break;
			oggit_bootstrap();
			oggit_update_worker_status("active", NULL);
			oggit_run_decode_loop();
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			MemoryContext oldcontext;
			char	   *last_error;

			/*
			 * CopyErrorData/pstrdup must not land in ErrorContext: FlushErrorState
			 * resets it and would leave last_error dangling, then pfree SIGSEGVs.
			 * Keep the copy in the long-lived worker context across abort/retry.
			 */
			Assert(oggit_worker_cxt != NULL);
			oldcontext = MemoryContextSwitchTo(oggit_worker_cxt);
			edata = CopyErrorData();
			last_error = (edata != NULL && edata->message != NULL)
				? pstrdup(edata->message) : pstrdup("oggit worker error");
			MemoryContextSwitchTo(oldcontext);

			g_instance.pid_cxt.OggitWorkerReady = false;

			/* Abort the failed transaction and log the error. */
			EmitErrorReport();
			FlushErrorState();
			if (edata != NULL)
				FreeErrorData(edata);
			if (t_thrd.slot_cxt.MyReplicationSlot != NULL)
				ReplicationSlotRelease();
			AbortOutOfAnyTransaction();

			/* Drop any partially-buffered batch events and accounting. */
			oggit_reset_batch();

			if (oggit_shutdown_requested)
			{
				pfree(last_error);
				break;
			}

			oggit_update_worker_status("failed", last_error);
			pfree(last_error);
			elog(LOG, "oggit worker: retrying after error");

			/* Back off before retrying. */
			(void) WaitLatch(&t_thrd.proc->procLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							 OGGIT_IDLE_NAP_MS);
			ResetLatch(&t_thrd.proc->procLatch);
		}
		PG_END_TRY();

		/* Normal exit from the decode loop only happens on a shutdown signal. */
		if (oggit_shutdown_requested)
			break;
	}

	g_instance.pid_cxt.OggitWorkerReady = false;
	elog(LOG, "oggit worker exiting");
	elog(LOG, "oggit worker exited gracefully");
	proc_exit(0);
}
