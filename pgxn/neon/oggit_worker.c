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
