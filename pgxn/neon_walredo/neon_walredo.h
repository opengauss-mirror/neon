#ifndef NEON_WALREDO_H
#define NEON_WALREDO_H

#include "postgres.h"
#include "storage/bufmgr.h"

/* 
 * Global variable to track the buffer containing the reconstructed page.
 * This is set by redo functions when they restore a page from FPI or 
 * otherwise reconstruct a page. The walredo infrastructure uses this
 * to know which buffer to return in GetPage.
 */
#ifdef __cplusplus
extern "C" {
#endif

extern Buffer neon_wal_redo_buffer;

#ifdef __cplusplus
}
#endif

#endif /* NEON_WALREDO_H */


