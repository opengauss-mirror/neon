/*-------------------------------------------------------------------------
 *
 * oggit_worker.h
 *	  Compute-side oggit logical decoding background worker.
 *
 * The oggit worker runs inside the compute (openGauss) process, not in the
 * control_plane. It drives logical decoding of the branch WAL through the
 * neon_oggit output plugin and persists the structured change events into the
 * oggit.* metadata tables used by incremental branch diff/merge.
 *
 * WAL is fetched from safekeepers through the Neon WAL reader: the decoding
 * context's page_read callback is overridden by Custom_XLogReaderRoutines
 * (NeonWALPageRead), which keeps a long-lived START_REPLICATION PHYSICAL
 * connection to a safekeeper and blocks on the socket until new WAL arrives.
 * The worker therefore consumes WAL in an event-driven loop rather than
 * polling pg_logical_slot_get_changes() on a timer.
 *
 *-------------------------------------------------------------------------
 */
#ifndef OGGIT_WORKER_H
#define OGGIT_WORKER_H

#ifdef __cplusplus
extern "C" {
#endif

/* Stage 1 (_PG_init): register neon.oggit_* GUCs. */
extern void pg_init_oggit(void);

/*
 * Worker entry point. The postmaster starts a dedicated OGGITWORKER kernel
 * thread and resolves this symbol via load_external_function("neon",
 * "OggitWorkerMain"), mirroring how WalProposerMain is launched.
 */
extern PGDLLEXPORT void OggitWorkerMain(Datum main_arg);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif							/* OGGIT_WORKER_H */