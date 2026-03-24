/*-------------------------------------------------------------------------
 *
 * walredoproc.c
 *	  Entry point for WAL redo helper
 *
 *
 * This file contains an alternative main() function for the 'postgres'
 * binary. In the special mode, we go into a special mode that's similar
 * to the single user mode. We don't launch postmaster or any auxiliary
 * processes. Instead, we wait for command from 'stdin', and respond to
 * 'stdout'.
 *
 * The protocol through stdin/stdout is loosely based on the libpq protocol.
 * The process accepts messages through stdin, and each message has the format:
 *
 * char   msgtype;
 * int32  length; // length of message including 'length' but excluding
 *                // 'msgtype', in network byte order
 * <payload>
 *
 * There are three message types:
 *
 * BeginRedoForBlock ('B'): Prepare for WAL replay for given block
 * PushPage ('P'): Copy a page image (in the payload) to buffer cache
 * ApplyRecord ('A'): Apply a WAL record (in the payload)
 * GetPage ('G'): Return a page image from buffer cache.
 * Ping ('H'): Return the input message.
 *
 * Currently, you only get a response to GetPage requests; the response is
 * simply a 8k page, without any headers. Errors are logged to stderr.
 *
 * FIXME:
 * - this currently requires a valid PGDATA, and creates a lock file there
 *   like a normal postmaster. There's no fundamental reason for that, though.
 * - should have EndRedoForBlock, and flush page cache, to allow using this
 *   mechanism for more than one block without restarting the process.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "../neon/neon_pgversioncompat.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#if defined(HAVE_LIBSECCOMP) && defined(__GLIBC__)
#define MALLOC_NO_MMAP
#include <malloc.h>
#endif

#if PG_MAJORVERSION_NUM < 16
#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif
#endif

#include "pgxc/locator.h"
#include "access/clog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "commands/async.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "knl/knl_variable.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#if PG_MAJORVERSION_NUM >= 17
#include "storage/dsm_registry.h"
#endif
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/smgr/smgr.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/inval.h"
#include "utils/catcache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#ifdef ENABLE_NEON
#include "executor/node/nodeShareInputScan.h"
#endif
#include "inmem_smgr.h"

#ifdef HAVE_LIBSECCOMP
#include "neon_seccomp.h"
#endif

PG_MODULE_MAGIC;

static int	ReadRedoCommand(StringInfo inBuf);
static void BeginRedoForBlock(StringInfo input_message);
static void PushPage(StringInfo input_message);
static void ApplyRecord(StringInfo input_message);
static void apply_error_callback(void *arg);
static bool redo_block_filter(XLogReaderState *record, uint8 block_id);
static void redo_buffer_allocated(Buffer buf);
static void GetPage(StringInfo input_message);
static void Ping(StringInfo input_message);
static ssize_t buffered_read(void *buf, size_t count);
#ifdef ENABLE_NEON
static void CreateFakeSharedMemoryAndSemaphores(bool makePrivate, int port);
#else
	static void CreateFakeSharedMemoryAndSemaphores(void);
#endif

static BufferTag target_redo_tag;

#ifdef ENABLE_NEON
/* openGauss doesn't have wal_redo_buffer as a global variable */
static Buffer wal_redo_buffer;

/* openGauss doesn't have xlog_outdesc, implement a compatible version */
static void
xlog_outdesc(StringInfo buf, XLogReaderState *record)
{
	RmgrId		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record);
	const char *id;

	appendStringInfoString(buf, RmgrTable[rmid].rm_name);
	appendStringInfoChar(buf, '/');

	/* openGauss uses xlog_type_name instead of rm_identify */
	if (RmgrTable[rmid].rm_type_name != NULL)
		id = RmgrTable[rmid].rm_type_name(info);
	else
		id = NULL;

	if (id == NULL)
		appendStringInfo(buf, "UNKNOWN (%X): ", info & ~XLR_INFO_MASK);
	else
		appendStringInfo(buf, "%s: ", id);

	RmgrTable[rmid].rm_desc(buf, record);
}
#endif

static XLogReaderState *reader_state;

#define TRACE DEBUG1

#ifdef HAVE_LIBSECCOMP


/*
 * https://man7.org/linux/man-pages/man2/close_range.2.html
 *
 * The `close_range` syscall is available as of Linux 5.9.
 *
 * The `close_range` libc wrapper is only available in glibc >= 2.34.
 * Debian Bullseye ships a libc package based on glibc 2.31.
 * => write the wrapper ourselves, using the syscall number from the kernel headers.
 *
 * If the Linux uAPI headers don't define the system call number,
 * fail the build deliberately rather than ifdef'ing it to ENOSYS.
 * We prefer a compile time over a runtime error for walredo.
 */
#include <unistd.h>
#include <sys/syscall.h>
#include <errno.h>

static int
close_range_syscall(unsigned int start_fd, unsigned int count, unsigned int flags)
{
    // return syscall(__NR_close_range, start_fd, count, flags);
	// TODO: MUST FIX IT!!!
	return 0;
}


static PgSeccompRule allowed_syscalls[] =
{
	/* Hard requirements */
	PG_SCMP_ALLOW(exit_group),
	PG_SCMP_ALLOW(pselect6),
	PG_SCMP_ALLOW(read),
	PG_SCMP_ALLOW(select),
	PG_SCMP_ALLOW(write),

	/* Memory allocation */
	PG_SCMP_ALLOW(brk),
#ifndef MALLOC_NO_MMAP
	/* TODO: musl doesn't have mallopt */
	PG_SCMP_ALLOW(mmap),
	PG_SCMP_ALLOW(munmap),
#endif
	/*
	 * getpid() is called on assertion failure, in ExceptionalCondition.
	 * It's not really needed, but seems pointless to hide it either. The
	 * system call unlikely to expose a kernel vulnerability, and the PID
	 * is stored in MyProcPid anyway.
	 */
	PG_SCMP_ALLOW(getpid),
	PG_SCMP_ALLOW(futex), /* needed for errbacktrace */

	/* Enable those for a proper shutdown. */
#if 0
	   PG_SCMP_ALLOW(munmap),
	   PG_SCMP_ALLOW(shmctl),
	   PG_SCMP_ALLOW(shmdt),
	   PG_SCMP_ALLOW(unlink),	/* shm_unlink */
#endif
};

static void
enter_seccomp_mode(void)
{
	/*
	 * The pageserver process relies on us to close all the file descriptors
	 * it potentially leaked to us, _before_ we start processing potentially dangerous
	 * wal records. See the comment in the Rust code that launches this process.
	 */
	if (close_range_syscall(3, ~0U, 0) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: could not close files >= fd 3")));

#ifdef MALLOC_NO_MMAP
	/* Ask glibc not to use mmap() */
	mallopt(M_MMAP_MAX, 0);
#endif

	seccomp_load_rules(allowed_syscalls, lengthof(allowed_syscalls));
}
#endif /* HAVE_LIBSECCOMP */
#ifdef __cplusplus
extern "C" {
#endif
	PGDLLEXPORT void
	WalRedoMain(int argc, char *argv[]);
#ifdef __cplusplus
}
#endif
/*
 * Entry point for the WAL redo process.
 *
 * Performs similar initialization as PostgresMain does for normal
 * backend processes. Some initialization was done in CallExtMain
 * already.
 */
PGDLLEXPORT void
WalRedoMain(int argc, char *argv[])
{
	int			firstchar;
	StringInfoData input_message;
#ifdef HAVE_LIBSECCOMP
	bool		enable_seccomp;
#endif

	t_thrd.xlog_cxt.am_wal_redo_postgres = true;
	/*
	 * Pageserver treats any output to stderr as an ERROR, so we must
	 * set the log level as early as possible to only log FATAL and 
	 * above during WAL redo (note that loglevel ERROR also logs LOG,
	 * which is super strange but that's not something we can solve
	 * for here. ¯\_(-_-)_/¯
	 */
	SetConfigOption("log_min_messages", "WARNING", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("client_min_messages", "ERROR", PGC_SUSET,
					PGC_S_OVERRIDE);

	/*
	 * WAL redo does not need a large number of buffers. And speed of
	 * DropRelationAllLocalBuffers() is proportional to the number of
	 * buffers. So let's keep it small (default value is 1024)
	 */
	/*
	 * install the simple in-memory smgr
	 */
	smgr_hook = smgr_inmem;
	smgr_init_hook = smgr_init_inmem;

#if PG_VERSION_NUM >= 160000
	/* make rmgr registry believe we can register the resource manager */
	process_shared_preload_libraries_in_progress = true;
	load_file("$libdir/neon_rmgr", false);
	process_shared_preload_libraries_in_progress = false;
#endif

#if PG_VERSION_NUM >= 150000
	process_shmem_requests();
	InitializeShmemGUCs();

	/*
	 * This will try to access data directory which we do not set.
	 * Seems to be pretty safe to disable.
	 */
	/* InitializeWalConsistencyChecking(); */
#endif

	/*
	 * Disable openGauss memory protection for walredo process.
	 * This prevents unbounded memory growth from dynamic memory tracking.
	 * The walredo process is short-lived and handles single WAL records,
	 * so memory protection overhead is unnecessary and harmful.
	 */
	g_instance.attr.attr_memory.enable_memory_limit = false;

	/*
	 * We have our own version of CreateSharedMemoryAndSemaphores() that
	 * sets up local memory instead of shared one.
	 */
#ifdef ENABLE_NEON
	CreateFakeSharedMemoryAndSemaphores(false, 0);
#else
	CreateFakeSharedMemoryAndSemaphores();
#endif

#if defined(ENABLE_NEON) || defined(PGXC)
	/*
	 * openGauss incremental checkpoint is incompatible with Neon's page
	 * server model and causes walredo to spin/hang. Disable it unconditionally.
	 */
	g_instance.attr.attr_storage.enableIncrementalCheckpoint = false;
#endif
	/*
	 * Remember stand-alone backend startup time,roughly at the same point
	 * during startup that postmaster does so.
	 */
	t_thrd.time_cxt.pg_start_time = GetCurrentTimestamp();

	/*
	 * Create a per-backend PGPROC struct in shared memory. We must do
	 * this before we can use LWLocks.
	 */
	InitAuxiliaryProcess();

	SetProcessingMode(NormalProcessing);

	/* Redo routines won't work if we're not "in recovery" */
	t_thrd.xlog_cxt.InRecovery = true;

	/*
	 * Create the memory context we will use in the main loop.
	 *
	 * MessageContext is reset once per iteration of the main loop, ie, upon
	 * completion of processing of each command message from the client.
	 */
	t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
													   "MessageContext",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);

	/* we need a ResourceOwner to hold buffer pins */
	Assert(t_thrd.utils_cxt.CurrentResourceOwner == NULL);
	t_thrd.utils_cxt.CurrentResourceOwner =
		ResourceOwnerCreate(NULL, "wal redo", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

	/* Initialize resource managers */
	for (int rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}
	reader_state = XLogReaderAllocate(&XLogPageRead, NULL);

#ifdef HAVE_LIBSECCOMP
	/* We prefer opt-out to opt-in for greater security */
	enable_seccomp = true;
	for (int i = 1; i < argc; i++)
		if (strcmp(argv[i], "--disable-seccomp") == 0)
			enable_seccomp = false;

	/*
	 * We deliberately delay the transition to the seccomp mode
	 * until it's time to enter the main processing loop;
	 * else we'd have to add a lot more syscalls to the allowlist.
	 */
	if (enable_seccomp)
		enter_seccomp_mode();
#endif /* HAVE_LIBSECCOMP */

	/*
	 * Main processing loop
	 */
	MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
	initStringInfo(&input_message);
#if PG_MAJORVERSION_NUM >= 16
	MyBackendType = B_BACKEND;
#endif

	for (;;)
	{
		/*
		 * Release memory left over from prior query cycle.
		 * This is CRITICAL for preventing memory leaks during WAL redo.
		 * Without this reset, each ApplyRecord call accumulates memory
		 * indefinitely, causing OOM during high-volume workloads like TPC-C.
		 */
		MemoryContextReset(t_thrd.mem_cxt.msg_mem_cxt);
		initStringInfo(&input_message);

		set_ps_display("idle", false);

		/*
		 * (3) read a command (loop blocks here)
		 */
		firstchar = ReadRedoCommand(&input_message);
		switch (firstchar)
		{
			case 'B':			/* BeginRedoForBlock */
				BeginRedoForBlock(&input_message);
				break;

			case 'P':			/* PushPage */
				PushPage(&input_message);
				break;

			case 'A':			/* ApplyRecord */
				ApplyRecord(&input_message);
				/*
				 * After applying a WAL record, ensure we're back in the
				 * message memory context. rm_redo functions in openGauss
				 * may switch to other memory contexts during execution.
				 */
				MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
				break;

			case 'G':			/* GetPage */
				GetPage(&input_message);
				break;

			case 'H': 			/* Ping */
				Ping(&input_message);
				break;

				/*
				 * EOF means we're done. Perform normal shutdown.
				 */
			case EOF:
				ereport(LOG,
						(errmsg("received EOF on stdin, shutting down")));

#ifdef HAVE_LIBSECCOMP
				/*
				 * Skip the shutdown sequence, leaving some garbage behind.
				 * Hopefully, postgres will clean it up in the next run.
				 * This way we don't have to enable extra syscalls, which is nice.
				 * See enter_seccomp_mode() above.
				 */
				if (enable_seccomp)
					_exit(0);
#endif /* HAVE_LIBSECCOMP */
				/*
				 * NOTE: if you are tempted to add more code here, DON'T!
				 * Whatever you had in mind to do should be set up as an
				 * on_proc_exit or on_shmem_exit callback, instead. Otherwise
				 * it will fail to be called during other backend-shutdown
				 * scenarios.
				 */
				proc_exit(0);

			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid frontend message type %d",
								firstchar)));
		}
	}							/* end of input-reading loop */
}


/*
 * Initialize dummy shmem.
 *
 * This code follows CreateSharedMemoryAndSemaphores() but manually sets up
 * the shmem header and skips few initialization steps that are not needed for
 * WAL redo.
 *
 * I've also tried removing most of initialization functions that request some
 * memory (like ApplyLauncherShmemInit and friends) but in reality it haven't had
 * any sizeable effect on RSS, so probably such clean up not worth the risk of having
 * half-initialized postgres.
 */
#ifdef ENABLE_NEON
#include "access/csnlog.h"
#include "utils/guc_storage.h"
#include "postmaster/startup.h"
void CreateFakeSharedMemoryAndSemaphores(bool makePrivate, int port){
	PGShmemHeader *hdr;
	char		cwd[MAXPGPATH];
    InitNuma();

    /*
     * MEMORY for WAL redo (stability-first configuration)
     * 
     * openGauss CR Buffer needs ~100MB (12800 * 8KB).
     * Allocate 192MB to ensure stability during TPC-C.
     * Memory optimization can be done later.
     */
    g_instance.attr.attr_storage.NBuffers = 16;
    g_instance.attr.attr_storage.enableIncrementalCheckpoint = false;
    
    CalcMaxBackends();

    int numSemas;
    Size size = 192 * 1024 * 1024;
    ereport(LOG, (errmsg("[neon-walredo] optimized shmem: %lu MB (CR Buffer disabled)", 
                         (unsigned long)(size/1024/1024))));

    /* Initialize the Memory Protection feature */
    gs_memprot_init(size);

	{
		hdr = (PGShmemHeader *) malloc(size);
		if (!hdr)
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("[neon-wal-redo] can not allocate (pseudo-) shared memory")));

		hdr->creatorPID = getpid();
		hdr->magic = PGShmemMagic;
		// hdr->dsm_control = 0;
		hdr->device = 42; /* not relevant for non-shared memory */
		hdr->inode = 43; /* not relevant for non-shared memory */
		hdr->totalsize = size;
		hdr->freeoffset = MAXALIGN(sizeof(PGShmemHeader));

		UsedShmemSegAddr = hdr;
		UsedShmemSegID = (unsigned long) 42; /* not relevant for non-shared memory */
	}

    InitShmemAccess(hdr);

        /*
         * Create semaphores
         */
    numSemas = ProcGlobalSemas();
    numSemas += SpinlockSemas();
    numSemas += XLogSemas();

#ifdef ENABLED_DEBUG_SYNC
    numSemas += 1; /* For debug sync handling */
#endif
    numSemas += 1; /* for locale concurrency control */

	if (!getcwd(cwd, MAXPGPATH))
		ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("[neon-wal-redo] can not read current directory name")));
	t_thrd.proc_cxt.DataDir = cwd;
	PGReserveSemaphores(numSemas, port);

	InitShmemAllocation();
	CreateLWLocks();
	InitShmemIndex();
#ifdef DEBUG_UHEAP
        UHeapSchemeInit();
#endif
	    {
        /* Manually init XLogCtl (skip XLOGShmemInit which accesses xlog dir) */
        {
            bool found;
            t_thrd.shemem_ptr_cxt.ControlFile = (ControlFileData *)ShmemInitStruct(
                "Control File", sizeof(ControlFileData), &found);
            memset(t_thrd.shemem_ptr_cxt.ControlFile, 0, sizeof(ControlFileData));
            t_thrd.shemem_ptr_cxt.XLogCtl = (XLogCtlData *)ShmemInitStruct(
                "XLOG Ctl", sizeof(XLogCtlData), &found);
            memset(t_thrd.shemem_ptr_cxt.XLogCtl, 0, sizeof(XLogCtlData));
            t_thrd.shemem_ptr_cxt.XLogCtl->SharedRecoveryInProgress = true;
        }
        CLOGShmemInit();
        CSNLOGShmemInit();
        MultiXactShmemInit();
        
        /*
         * CRITICAL FIX: Initialize g_instance pointers before InitBufferPool.
         * InitBufferPool accesses g_instance.ckpt_cxt_ctl which is NULL by default.
         * Without this, we get SIGSEGV when accessing g_instance.ckpt_cxt_ctl->CkptBufferIds.
         */
        g_instance.ckpt_cxt_ctl = &g_instance.ckpt_cxt;
        /* Ensure HTAP counter is 0 so HAVE_HTAP_TABLES returns false */
        pg_atomic_init_u32(&g_instance.imcstore_cxt.imcs_tbl_cnt, 0);
        InitBufferPool();
        pca_buf_init_ctx();
        // /* global temporay table */
        // active_gtt_shared_hash_init();
        /*
         * Set up lock manager
         */
        InitLocks();

        /*
         * Set up predicate lock manager
         */
        InitPredicateLocks();

        SSInitTxnStatusCache();
        // SSInitXminInfo();
    }
	  if (!IsUnderPostmaster) {
		InitializeNumLwLockPartitions();
        InitProcGlobal();
        // InitBgworkerGlobal();
        CreateSharedProcArray();
        CreateProcXactHashTable();
    }

    /*
     * Restore necessary initializations for WAL redo stability.
     * Some rm_redo functions may access these structures.
     */
    CreateSharedBackendStatus();
    
    TwoPhaseShmemInit();
    
    /* Set up shared-inval messaging - needed by some catalog operations */
    CreateSharedInvalidationState();
    
    /* Set up interprocess signaling mechanisms */
    PMSignalShmemInit();
    ProcSignalShmemInit();
    
    /* BTree and SyncScan - may be accessed by index redo */
    BTreeShmemInit();
    SyncScanShmemInit();
    
    /* Initialize data file cache - needed for smgr */
    InitDataFileIdCache();
    
    ereport(LOG, (errmsg("[neon-wal-redo] shared memory initialization complete")));
}
#else
static void
CreateFakeSharedMemoryAndSemaphores(void)
{
	PGShmemHeader *hdr;
	Size		size;
	int			numSemas;
	char		cwd[MAXPGPATH];

#if PG_VERSION_NUM >= 150000
	size = CalculateShmemSize(&numSemas);
#else
	/*
	 * Postgres v14 doesn't have a separate CalculateShmemSize(). Use result of the
	 * corresponging calculation in CreateSharedMemoryAndSemaphores()
	 */
	size = 1409024;
	numSemas = 10;
#endif

	/* Dummy implementation of PGSharedMemoryCreate() */
	{
		hdr = (PGShmemHeader *) malloc(size);
		if (!hdr)
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("[neon-wal-redo] can not allocate (pseudo-) shared memory")));

		hdr->creatorPID = getpid();
		hdr->magic = PGShmemMagic;
		// hdr->dsm_control = 0;
		hdr->device = 42; /* not relevant for non-shared memory */
		hdr->inode = 43; /* not relevant for non-shared memory */
		hdr->totalsize = size;
		hdr->freeoffset = MAXALIGN(sizeof(PGShmemHeader));

		UsedShmemSegAddr = hdr;
		UsedShmemSegID = (unsigned long) 42; /* not relevant for non-shared memory */
	}

	InitShmemAccess(hdr);

	/*
	 * Reserve semaphores uses dir name as a source of entropy. Set it to cwd(). Rest
	 * of the code does not need DataDir access so nullify DataDir after
	 * PGReserveSemaphores() to error out if something will try to access it.
	 */
	if (!getcwd(cwd, MAXPGPATH))
		ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("[neon-wal-redo] can not read current directory name")));
	t_thrd.proc_cxt.DataDir = cwd;
	PGReserveSemaphores(numSemas, 0);
	t_thrd.proc_cxt.DataDir = NULL;

	/*
	 * The rest of function follows CreateSharedMemoryAndSemaphores() closely,
	 * skipped parts are marked with comments.
	 */
	InitShmemAllocation();

	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	/*
	 * Set up xlog, clog, and buffers
	 */
#if PG_MAJORVERSION_NUM >= 17
	DSMRegistryShmemInit();
	VarsupShmemInit();
#endif
	XLOGShmemInit();
	CLOGShmemInit();
	MultiXactShmemInit();
	InitBufferPool();

	/*
	 * Set up lock manager
	 */
	InitLocks();

	/*
	 * Set up predicate lock manager
	 */
	InitPredicateLocks();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();
	CreateSharedProcArray();
	CreateSharedBackendStatus();
	TwoPhaseShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	CreateSharedInvalidationState();

	/*
	 * OPTIMIZED: Only essential signaling for WAL redo
	 * Removed unnecessary: Checkpointer, AutoVacuum, Replication, WalSnd/Rcv
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	/* CheckpointerShmemInit();    -- not needed for redo */
	/* AutoVacuumShmemInit();      -- not needed for redo */
	/* ReplicationSlotsShmemInit();-- not needed for redo */
	/* ReplicationOriginShmemInit(); -- not needed for redo */
	/* WalSndShmemInit();          -- not needed for redo */
	/* WalRcvShmemInit();          -- not needed for redo */
	/* ApplyLauncherShmemInit();   -- not needed for redo */

	/*
	 * BTree and SyncScan needed for rm_redo index operations
	 */
	BTreeShmemInit();
	SyncScanShmemInit();


#ifdef EXEC_BACKEND

	/*
	 * Alloc the win32 shared backend array
	 * In openGauss environment, we need to provide a stub implementation
	 * or avoid calling this function as it's not defined in neon_walredo module
	 */
	if (!IsUnderPostmaster) {
#ifdef ENABLE_NEON
		/* In openGauss, we skip this call as it's not implemented in the module */
		elog(DEBUG1, "Skipping ShmemBackendArrayAllocation() in openGauss environment");
#else
		ShmemBackendArrayAllocation();
#endif
	}
#endif

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (t_thrd.storage_cxt.shmem_startup_hook)
		t_thrd.storage_cxt.shmem_startup_hook();
}
#endif

/* Version compatility wrapper for ReadBufferWithoutRelcache */
static inline Buffer
NeonRedoReadBuffer(NRelFileInfo rinfo,
		   ForkNumber forkNum, BlockNumber blockNum,
		   ReadBufferMode mode)
{
#ifdef ENABLE_NEON
	/* openGauss version requires XLogPhyBlock* as last parameter */
	return ReadBufferWithoutRelcache(rinfo, forkNum, blockNum, mode,
									 NULL, /* no strategy */
									 NULL); /* no XLogPhyBlock */
#elif PG_VERSION_NUM >= 150000
	return ReadBufferWithoutRelcache(rinfo, forkNum, blockNum, mode,
									 NULL, /* no strategy */
									 true); /* WAL redo is only performed on permanent rels */
#else
	return ReadBufferWithoutRelcache(rinfo, forkNum, blockNum, mode,
									 NULL); /* no strategy */
#endif
}


/*
 * Some debug function that may be handy for now.
 */
static char * __attribute__((unused))
pprint_buffer(char *data, int len)
{
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfo(&s, "\n");
	for (int i = 0; i < len; i++) {

		appendStringInfo(&s, "%02x ", (*(((char *) data) + i) & 0xff) );
		if (i % 32 == 31) {
			appendStringInfo(&s, "\n");
		}
	}
	appendStringInfo(&s, "\n");

	return s.data;
}

/* ----------------------------------------------------------------
 *		routines to obtain user input
 * ----------------------------------------------------------------
 */

/*
 * Read next command from the client.
 *
 *	the string entered by the user is placed in its parameter inBuf,
 *	and we act like a Q message was received.
 *
 *	EOF is returned if end-of-file input is seen; time to shut down.
 * ----------------
 */
static int
ReadRedoCommand(StringInfo inBuf)
{
	ssize_t		ret;
	char		hdr[1 + sizeof(int32)];
	int			qtype;
	int32		len;

	/* Read message type and message length */
	ret = buffered_read(hdr, sizeof(hdr));
	if (ret != sizeof(hdr))
	{
		if (ret == 0)
			return EOF;
		else if (ret < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not read message header: %m")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF")));
	}

	qtype = hdr[0];
	memcpy(&len, &hdr[1], sizeof(int32));
#ifdef ENABLE_NEON
	/* openGauss doesn't have pg_ntoh32, use standard ntohl */
	len = ntohl(len);
#else
	len = pg_ntoh32(len);
#endif

	if (len < 4)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));

	len -= 4;					/* discount length itself */

	/* Read the message payload */
	enlargeStringInfo(inBuf, len);
	ret = buffered_read(inBuf->data, len);
	if (ret != len)
	{
		if (ret < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not read message: %m")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF")));
	}
	inBuf->len = len;
	inBuf->data[len] = '\0';

	return qtype;
}

/*
 * Prepare for WAL replay on given block
 */
static void
BeginRedoForBlock(StringInfo input_message)
{
	NRelFileInfo rinfo;
	ForkNumber forknum;
	BlockNumber blknum;
	SMgrRelation reln;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 */
	forknum = pq_getmsgbyte(input_message);
#if PG_MAJORVERSION_NUM < 16
	rinfo.spcNode = pq_getmsgint(input_message, 4);
	rinfo.dbNode = pq_getmsgint(input_message, 4);
	rinfo.relNode = pq_getmsgint(input_message, 4);
#ifdef ENABLE_NEON
	/* openGauss RelFileNode has extra fields that must be initialized */
	rinfo.bucketNode = InvalidBktId;
	rinfo.opt = 0;
#endif
#else
	rinfo.spcOid = pq_getmsgint(input_message, 4);
	rinfo.dbOid = pq_getmsgint(input_message, 4);
	rinfo.relNumber = pq_getmsgint(input_message, 4);
#endif
	blknum = pq_getmsgint(input_message, 4);
	wal_redo_buffer = InvalidBuffer;

	InitBufferTag(&target_redo_tag, &rinfo, forknum, blknum);

	elog(TRACE, "BeginRedoForBlock %u/%u/%u.%d blk %u",
		 RelFileInfoFmt(rinfo),
		 target_redo_tag.forkNum,
		 target_redo_tag.blockNum);

	reln = smgropen(rinfo, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT);
#ifdef ENABLE_NEON
	/* openGauss: smgr_cached_nblocks is a single value, only for MAIN_FORKNUM */
	if (forknum == MAIN_FORKNUM)
	{
		if (reln->smgr_cached_nblocks == InvalidBlockNumber ||
			reln->smgr_cached_nblocks < blknum + 1)
		{
			reln->smgr_cached_nblocks = blknum + 1;
		}
	}
#else
	/* PostgreSQL: smgr_cached_nblocks is an array */
	if (reln->smgr_cached_nblocks[forknum] == InvalidBlockNumber ||
		reln->smgr_cached_nblocks[forknum] < blknum + 1)
	{
		reln->smgr_cached_nblocks[forknum] = blknum + 1;
	}
#endif
}

/*
 * Receive a page given by the client, and put it into buffer cache.
 */
static void
PushPage(StringInfo input_message)
{
	NRelFileInfo rinfo;
	ForkNumber forknum;
	BlockNumber blknum;
	const char *content;
	Buffer		buf;
	Page		page;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 * 8k page content
	 */
	forknum = pq_getmsgbyte(input_message);
#if PG_MAJORVERSION_NUM < 16
	rinfo.spcNode = pq_getmsgint(input_message, 4);
	rinfo.dbNode = pq_getmsgint(input_message, 4);
	rinfo.relNode = pq_getmsgint(input_message, 4);
#ifdef ENABLE_NEON
	/* openGauss RelFileNode has extra fields that must be initialized */
	rinfo.bucketNode = InvalidBktId;
	rinfo.opt = 0;
#endif
#else
	rinfo.spcOid = pq_getmsgint(input_message, 4);
	rinfo.dbOid = pq_getmsgint(input_message, 4);
	rinfo.relNumber = pq_getmsgint(input_message, 4);
#endif
	blknum = pq_getmsgint(input_message, 4);
	content = pq_getmsgbytes(input_message, BLCKSZ);

	buf = NeonRedoReadBuffer(rinfo, forknum, blknum, RBM_ZERO_AND_LOCK);
	wal_redo_buffer = buf;
	page = BufferGetPage(buf);
	memcpy(page, content, BLCKSZ);
	MarkBufferDirty(buf); /* pro forma */
	UnlockReleaseBuffer(buf);
}

/*
 * Receive a WAL record, and apply it.
 *
 * All the pages should be loaded into the buffer cache by PushPage calls already.
 */
static void
ApplyRecord(StringInfo input_message)
{
	char	   *errormsg;
	XLogRecPtr	lsn;
	XLogRecord *record;
	int			nleft;
	ErrorContextCallback errcallback;
#if PG_VERSION_NUM >= 150000
	DecodedXLogRecord *decoded;
#define STATIC_DECODEBUF_SIZE (64 * 1024)
	static char *static_decodebuf = NULL;
	size_t		required_space;
#endif

	/*
	 * message format:
	 *
	 * LSN (the *end* of the record)
	 * record
	 */
	lsn = pq_getmsgint64(input_message);

	/*
	 * NOTE: Do NOT call smgrinit() here!
	 * 
	 * smgrinit() would reset inmem_smgr state (used_pages = 0), which would
	 * clear all pages stored by previous PushPage() calls. This was the root
	 * cause of the "zero page" corruption bug: the base image sent by pageserver
	 * via PushPage() was being cleared before rm_redo could read it.
	 * 
	 * The smgr cleanup is done properly in GetPage() after the page is returned.
	 */

	/* note: the input must be aligned here */
	record = (XLogRecord *) pq_getmsgbytes(input_message, sizeof(XLogRecord));

	nleft = input_message->len - input_message->cursor;
	if (record->xl_tot_len != sizeof(XLogRecord) + nleft)
		elog(ERROR, "mismatch between record (%d) and message size (%d)",
			 record->xl_tot_len, (int) sizeof(XLogRecord) + nleft);

	/* Setup error traceback support for ereport() */
	errcallback.callback = apply_error_callback;
	errcallback.arg = (void *) reader_state;
#ifdef ENABLE_NEON
	/* openGauss: error_context_stack is in t_thrd.log_cxt */
	errcallback.previous = t_thrd.log_cxt.error_context_stack;
	t_thrd.log_cxt.error_context_stack = &errcallback;
#else
	/* PostgreSQL: error_context_stack is a global variable */
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;
#endif

#ifdef ENABLE_NEON
	/* openGauss doesn't have XLogBeginRead, manually implement the same logic */
	ResetDecoder(reader_state);
	reader_state->EndRecPtr = lsn;
	reader_state->ReadRecPtr = InvalidXLogRecPtr;
#else
	/* PostgreSQL: use XLogBeginRead function */
	XLogBeginRead(reader_state, lsn);
#endif

#if PG_VERSION_NUM >= 150000
	/*
	 * For reasonably small records, reuse a fixed size buffer to reduce
	 * palloc overhead.
	 */
	required_space = DecodeXLogRecordRequiredSpace(record->xl_tot_len);
	if (required_space <= STATIC_DECODEBUF_SIZE)
	{
		if (static_decodebuf == NULL)
			static_decodebuf = MemoryContextAlloc(TopMemoryContext, STATIC_DECODEBUF_SIZE);
		decoded = (DecodedXLogRecord *) static_decodebuf;
	}
	else
		decoded = palloc(required_space);

	if (!DecodeXLogRecord(reader_state, decoded, record, lsn, &errormsg))
		elog(ERROR, "failed to decode WAL record: %s", errormsg);
	else
	{
		/* Record the location of the next record. */
		decoded->next_lsn = reader_state->NextRecPtr;

		/*
		 * Update the pointers to the beginning and one-past-the-end of this
		 * record, again for the benefit of historical code that expected the
		 * decoder to track this rather than accessing these fields of the record
		 * itself.
		 */
		reader_state->record = decoded;
		reader_state->ReadRecPtr = decoded->lsn;
		reader_state->EndRecPtr = decoded->next_lsn;
	}
#else
	/*
	 * In lieu of calling XLogReadRecord, store the record 'decoded_record'
	 * buffer directly.
	 */
	reader_state->ReadRecPtr = lsn;
	reader_state->decoded_record = record;
	if (!DecodeXLogRecord(reader_state, record, &errormsg))
		elog(ERROR, "failed to decode WAL record: %s", errormsg);
#endif

	/* Ignore any other blocks than the ones the caller is interested in */
	redo_read_buffer_filter = redo_block_filter;

	/* Register buffer allocation hook for will_init cases */
	redo_buffer_allocated_hook = redo_buffer_allocated;

	/*
	 * DIAGNOSTIC: Log the rm_redo call details.
	 * This helps trace which WAL records cause zero page issues.
	 */
	elog(DEBUG1, "[WALREDO_APPLY] rm_redo: rmid=%u info=0x%02X lsn=%X/%X "
				 "target_block=%u/%u/%u.%d blk=%u has_base_image=%s",
				 record->xl_rmid, record->xl_info,
				 (uint32) (lsn >> 32), (uint32) lsn,
				 target_redo_tag.rnode.spcNode,
				 target_redo_tag.rnode.dbNode,
				 target_redo_tag.rnode.relNode,
				 target_redo_tag.forkNum,
				 target_redo_tag.blockNum,
				 BufferIsValid(wal_redo_buffer) ? "yes" : "no");

	{
		struct timeval start_time, end_time;
		long elapsed_ms;
		gettimeofday(&start_time, NULL);

		RmgrTable[record->xl_rmid].rm_redo(reader_state);

		gettimeofday(&end_time, NULL);
		elapsed_ms = (end_time.tv_sec - start_time.tv_sec) * 1000 +
					 (end_time.tv_usec - start_time.tv_usec) / 1000;
		if (elapsed_ms > 100)
		{
			ereport(LOG,
					(errmsg("[WALREDO_SLOW] rm_redo took %ld ms for rmid=%u info=%u lsn=%X/%X",
							elapsed_ms, record->xl_rmid, record->xl_info,
							(uint32) (lsn >> 32), (uint32) lsn)));
		}
	}

	/*
	 * If no base image of the page was provided by PushPage, initialize
	 * wal_redo_buffer here. The first WAL record must initialize the page
	 * in that case.
	 *
	 * CRITICAL FIX: When will_init is true (pageserver didn't send base image),
	 * rm_redo should have initialized the page. If wal_redo_buffer is still
	 * InvalidBuffer at this point, it means rm_redo didn't properly handle
	 * the will_init case - it used RBM_NORMAL which returns BLK_NOTFOUND for
	 * zero pages instead of RBM_ZERO_AND_LOCK which would initialize the page.
	 *
	 * We use RBM_ZERO_AND_LOCK here to ensure the buffer exists, but log a
	 * warning because the page content may be incorrect (rm_redo was skipped).
	 */
	if (BufferIsInvalid(wal_redo_buffer))
	{
		/*
		 * No base image was provided (will_init=true case), but rm_redo
		 * didn't set wal_redo_buffer. This is likely because:
		 * 1. XLogReadBufferForRedo returned BLK_NOTFOUND for zero page
		 * 2. rm_redo skipped the operation
		 *
		 * Use RBM_ZERO_AND_LOCK to at least get a buffer, but warn that
		 * the page may be invalid.
		 */
		ereport(WARNING,
				(errmsg("[WALREDO_NOINIT] rm_redo did not initialize buffer for "
						"%u/%u/%u.%d blk %u (rmid=%u info=0x%02X lsn=%X/%X). "
						"This may result in zero page being returned.",
						target_redo_tag.rnode.spcNode,
						target_redo_tag.rnode.dbNode,
						target_redo_tag.rnode.relNode,
						target_redo_tag.forkNum,
						target_redo_tag.blockNum,
						record->xl_rmid, record->xl_info,
						(uint32) (lsn >> 32), (uint32) lsn)));

		wal_redo_buffer = NeonRedoReadBuffer(BufTagGetNRelFileInfo(target_redo_tag),
											 target_redo_tag.forkNum,
											 target_redo_tag.blockNum,
											 RBM_ZERO_AND_LOCK);
		if (!BufferIsInvalid(wal_redo_buffer))
		{
			/*
			 * We got a buffer but it's likely zero/uninitialized.
			 * The caller (GetPage) will detect this and log a warning.
			 */
			UnlockReleaseBuffer(wal_redo_buffer);
		}
		else
		{
			ereport(ERROR,
					(errmsg("[WALREDO_NOINIT] failed to get buffer for "
							"%u/%u/%u.%d blk %u even with RBM_ZERO_AND_LOCK",
							target_redo_tag.rnode.spcNode,
							target_redo_tag.rnode.dbNode,
							target_redo_tag.rnode.relNode,
							target_redo_tag.forkNum,
							target_redo_tag.blockNum)));
		}
	}

	redo_read_buffer_filter = NULL;
	redo_buffer_allocated_hook = NULL;

	/* Pop the error context stack */
#ifdef ENABLE_NEON
	/* openGauss: error_context_stack is in t_thrd.log_cxt */
	t_thrd.log_cxt.error_context_stack = errcallback.previous;
#else
	/* PostgreSQL: error_context_stack is a global variable */
	error_context_stack = errcallback.previous;
#endif

	elog(TRACE, "applied WAL record with LSN %X/%X",
		 (uint32) (lsn >> 32), (uint32) lsn);

#if PG_VERSION_NUM >= 150000
	if ((char *) decoded != static_decodebuf)
		pfree(decoded);
#endif
}

/*
 * Error context callback for errors occurring during ApplyRecord
 */
static void
apply_error_callback(void *arg)
{
	XLogReaderState *record = (XLogReaderState *) arg;
	StringInfoData buf;

	initStringInfo(&buf);
#if PG_VERSION_NUM >= 150000
	if (record->record)
#else
	if (record->decoded_record)
#endif
		xlog_outdesc(&buf, record);

	/* translator: %s is a WAL record description */
	errcontext("WAL redo at %X/%X for %s",
			   LSN_FORMAT_ARGS(record->ReadRecPtr),
			   buf.data);

	pfree(buf.data);
}



/*
 * Hook called when a buffer is allocated for the target block during WAL redo.
 * This is critical for will_init cases where no base image is provided by pageserver.
 * The buffer must be tracked so that GetPage can return it after rm_redo completes.
 */
static void
redo_buffer_allocated(Buffer buf)
{
	ereport(LOG, (errmsg("[WALREDO_BUFFER_HOOK] called: buf=%d, wal_redo_buffer=%d, buf_valid=%d, wrb_valid=%d",
		buf, wal_redo_buffer, BufferIsValid(buf), BufferIsValid(wal_redo_buffer))));
	if (BufferIsValid(buf) && !BufferIsValid(wal_redo_buffer))
	{
		wal_redo_buffer = buf;
		ereport(LOG, (errmsg("[WALREDO_BUFFER_HOOK] buffer %d set as wal_redo_buffer", buf)));
	}
}

static bool
redo_block_filter(XLogReaderState *record, uint8 block_id)
{
	BufferTag	target_tag;
	NRelFileInfo rinfo;
	bool result;
#ifdef ENABLE_NEON
	bool hasImage = XLogRecHasBlockImage(record, block_id);
#endif

#if PG_VERSION_NUM >= 150000
	XLogRecGetBlockTag(record, block_id,
					   &rinfo, &target_tag.forkNum, &target_tag.blockNum);
#else
	if (!XLogRecGetBlockTag(record, block_id,
							&rinfo, &target_tag.forkNum, &target_tag.blockNum))
	{
		/* Caller specified a bogus block_id */
		elog(PANIC, "failed to locate backup block with ID %d", block_id);
	}
#endif
	CopyNRelFileInfoToBufTag(target_tag, rinfo);

	/*
	 * Can a WAL redo function ever access a relation other than the one that
	 * it modifies? I don't see why it would.
	 * Custom RMGRs may be affected by this.
	 */
	if (!RelFileInfoEquals(rinfo, BufTagGetNRelFileInfo(target_redo_tag)))
		elog(WARNING, "REDO accessing unexpected page: %u/%u/%u.%u blk %u",
			 RelFileInfoFmt(rinfo), target_tag.forkNum, target_tag.blockNum);

	/*
	 * If this block isn't one we are currently restoring, then return 'true'
	 * so that this gets ignored
	 */
	result = !BufferTagsEqual(&target_tag, &target_redo_tag);

	return result;
}

/*
 * Get a page image back from buffer cache.
 *
 * After applying some records.
 */
static void
GetPage(StringInfo input_message)
{
	NRelFileInfo rinfo;
	ForkNumber forknum;
	BlockNumber blknum;
	Buffer		buf;
	Page		page;
	int			tot_written;

	/*
	 * message format:
	 *
	 * spcNode
	 * dbNode
	 * relNode
	 * ForkNumber
	 * BlockNumber
	 */
	forknum = pq_getmsgbyte(input_message);
#if PG_MAJORVERSION_NUM < 16
	rinfo.spcNode = pq_getmsgint(input_message, 4);
	rinfo.dbNode = pq_getmsgint(input_message, 4);
	rinfo.relNode = pq_getmsgint(input_message, 4);
#ifdef ENABLE_NEON
	/* openGauss RelFileNode has extra fields that must be initialized */
	rinfo.bucketNode = InvalidBktId;
	rinfo.opt = 0;
#endif
#else
	rinfo.spcOid = pq_getmsgint(input_message, 4);
	rinfo.dbOid = pq_getmsgint(input_message, 4);
	rinfo.relNumber = pq_getmsgint(input_message, 4);
#endif
	blknum = pq_getmsgint(input_message, 4);

	/* FIXME: check that we got a BeginRedoForBlock message or this earlier */

	buf = NeonRedoReadBuffer(rinfo, forknum, blknum, RBM_NORMAL);
	Assert(buf == wal_redo_buffer);
	page = BufferGetPage(buf);

	/*
	 * DIAGNOSTIC: Check for zero page before returning.
	 * A zero page (pd_lower=0 AND pd_upper=0) after WAL redo indicates
	 * that rm_redo did not properly initialize the page.
	 */
	{
		PageHeader phdr = (PageHeader) page;
		uint16 pd_lower = phdr->pd_lower;
		uint16 pd_upper = phdr->pd_upper;
		uint16 pd_special = phdr->pd_special;
		uint16 pd_pagesize_version = phdr->pd_pagesize_version;
		
		if (pd_lower == 0 && pd_upper == 0)
		{
			ereport(WARNING,
					(errmsg("[WALREDO_ZERO_PAGE] returning zero page for %u/%u/%u.%d blk %u: "
							"pd_lower=%u, pd_upper=%u, pd_special=%u, pd_pagesize_version=0x%04X",
							RelFileInfoFmt(rinfo), forknum, blknum,
							pd_lower, pd_upper, pd_special, pd_pagesize_version)));
		}
		else
		{
			elog(DEBUG1, "[WALREDO_PAGE] returning page for %u/%u/%u.%d blk %u: "
						 "pd_lower=%u, pd_upper=%u, pd_special=%u",
						 RelFileInfoFmt(rinfo), forknum, blknum,
						 pd_lower, pd_upper, pd_special);
		}
	}

	/* Response: Page content */
	tot_written = 0;
	do {
		ssize_t		rc;

		rc = write(STDOUT_FILENO, &page[tot_written], BLCKSZ - tot_written);
		if (rc < 0) {
			/* If interrupted by signal, just retry */
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to stdout: %m")));
		}
		tot_written += rc;
	} while (tot_written < BLCKSZ);

	ReleaseBuffer(buf);
	DropRelationAllLocalBuffers(rinfo);
	wal_redo_buffer = InvalidBuffer;

	/*
	 * CRITICAL: Release all resources held by the ResourceOwner.
	 * 
	 * openGauss accumulates resources (buffer pins, catcache refs, relation refs,
	 * etc.) in the ResourceOwner during WAL redo. Without explicit release,
	 * memory grows unbounded (~10GB/minute during TPC-C).
	 * 
	 * We call ResourceOwnerRelease in all three phases to ensure complete cleanup:
	 * - BEFORE_LOCKS: Release buffer pins, catcache refs, etc.
	 * - LOCKS: Release any locks (shouldn't have any in walredo)
	 * - AFTER_LOCKS: Final cleanup
	 */
	ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner,
						 RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS, true, true);

	/*
	 * Reset smgr state and any cached relations to prevent memory
	 * accumulation across redo cycles.
	 */
	smgrinit();
	smgrcloseall();

	/* Memory limit removed - proper optimization should keep memory low */

	elog(TRACE, "Page sent back for block %u", blknum);
}


static void
Ping(StringInfo input_message)
{
	int			tot_written;
	/* Response: the input message */
	tot_written = 0;
	do {
		ssize_t		rc;
		/* We don't need alignment, but it's bad practice to use char[BLCKSZ] */
#if PG_VERSION_NUM >= 160000
		const static PGIOAlignedBlock response = {0};
#else
		const static PGAlignedBlock response = {0};
#endif
		rc = write(STDOUT_FILENO, &response.data[tot_written], BLCKSZ - tot_written);
		if (rc < 0) {
			/* If interrupted by signal, just retry */
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to stdout: %m")));
		}
		tot_written += rc;
	} while (tot_written < BLCKSZ);

	elog(TRACE, "Page sent back for ping");
}


/* Buffer used by buffered_read() */
static char stdin_buf[16 * 1024];
static size_t stdin_len = 0;	/* # of bytes in buffer */
static size_t stdin_ptr = 0;	/* # of bytes already consumed */

/*
 * Like read() on stdin, but buffered.
 *
 * We cannot use libc's buffered fread(), because it uses syscalls that we
 * have disabled with seccomp(). Depending on the platform, it can call
 * 'fstat' or 'newfstatat'. 'fstat' is probably harmless, but 'newfstatat'
 * seems problematic because it allows interrogating files by path name.
 *
 * The return value is the number of bytes read. On error, -1 is returned, and
 * errno is set appropriately. Unlike read(), this fills the buffer completely
 * unless an error happens or EOF is reached.
 */
static ssize_t
buffered_read(void *buf, size_t count)
{
	char	   *dst = static_cast<char *>(buf);

	while (count > 0)
	{
		size_t		nthis;

		if (stdin_ptr == stdin_len)
		{
			ssize_t		ret;

			ret = read(STDIN_FILENO, stdin_buf, sizeof(stdin_buf));
			if (ret < 0)
			{
				/* don't do anything here that could set 'errno' */
				return ret;
			}
			if (ret == 0)
			{
				/* EOF */
				break;
			}
			stdin_len = (size_t) ret;
			stdin_ptr = 0;
		}
		nthis = Min(stdin_len - stdin_ptr, count);

		memcpy(dst, &stdin_buf[stdin_ptr], nthis);

		stdin_ptr += nthis;
		count -= nthis;
		dst += nthis;
	}

	return (dst - (char *) buf);
}
