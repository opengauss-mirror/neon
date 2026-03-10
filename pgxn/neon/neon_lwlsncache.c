#include "postgres.h"

#include "neon.h"
#include "neon_lwlsncache.h"

#include "miscadmin.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/buf/buf_internals.h"
#include "utils/guc.h"
#include "utils/hsearch.h"



typedef struct LastWrittenLsnCacheEntry
{
	BufferTag	key;
	XLogRecPtr	lsn;
	/* double linked list for LRU replacement algorithm */
	dlist_node	lru_node;
} LastWrittenLsnCacheEntry;

typedef struct LwLsnCacheCtl {
	int lastWrittenLsnCacheSize;
	/*
	* Maximal last written LSN for pages not present in lastWrittenLsnCache
	*/
	XLogRecPtr  maxLastWrittenLsn;

	/*
	* Double linked list to implement LRU replacement policy for last written LSN cache.
	* Access to this list as well as to last written LSN cache is protected by 'LastWrittenLsnLock'.
	*/
	dlist_head lastWrittenLsnLRU;
} LwLsnCacheCtl;


/*
 * Cache of last written LSN for each relation page.
 * Also to provide request LSN for smgrnblocks, smgrexists there is pseudokey=InvalidBlockId which stores LSN of last
 * relation metadata update.
 * Size of the cache is limited by GUC variable lastWrittenLsnCacheSize ("lsn_cache_size"),
 * pages are replaced using LRU algorithm, based on L2-list.
 * Access to this cache is protected by 'LastWrittenLsnLock'.
 *
 * NOTE: In openGauss, 'LastWrittenLsnLock' is defined as a macro in
 * lwlocknames.h that points into mainLWLockArray, so we must not redeclare
 * it here or assign to it. Just use the macro.
 */
static HTAB *lastWrittenLsnCache;

LwLsnCacheCtl* LwLsnCache;

static int lwlsn_cache_size = (128 * 1024);


static void
lwlc_register_gucs(void)
{
	DefineCustomIntVariable("neon.last_written_lsn_cache_size",
							"Size of last written LSN cache used by Neon",
							NULL,
							&lwlsn_cache_size,
							(128*1024), 1024, INT_MAX,
							PGC_POSTMASTER,
							0, /* plain units */
							NULL, NULL, NULL);
}

static XLogRecPtr SetLastWrittenLSNForBlockRangeInternal(XLogRecPtr lsn,
														 NRelFileInfo rlocator,
														 ForkNumber forknum,
														 BlockNumber from,
														 BlockNumber n_blocks);

/* All the necessary hooks are defined here */


/* These hold the set_lwlsn_* hooks which were installed before ours, if any */
static set_lwlsn_block_range_hook_type prev_set_lwlsn_block_range_hook = NULL;
static set_lwlsn_block_v_hook_type prev_set_lwlsn_block_v_hook = NULL;
static set_lwlsn_block_hook_type prev_set_lwlsn_block_hook = NULL;
static set_max_lwlsn_hook_type prev_set_max_lwlsn_hook = NULL;
static set_lwlsn_relation_hook_type prev_set_lwlsn_relation_hook = NULL;
static set_lwlsn_db_hook_type prev_set_lwlsn_db_hook = NULL;

static void neon_set_max_lwlsn(XLogRecPtr lsn);

void
init_lwlsncache(void)
{
	/*
	 * 在 PostgreSQL 中，本函数只应该在 shared_preload_libraries 阶段调用。
	 * 在接入 openGauss 时，某些进程可能通过其它路径加载 neon，
	 * 为了安全起见，这里不再直接 ERROR/abort，而是发出 WARNING 并直接返回。
	 * 具体调用时机由 _PG_init 中的判断控制。
	 */
	if (!u_sess->misc_cxt.process_shared_preload_libraries_in_progress)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("init_lwlsncache called when shared preload libraries are not being loaded; skipping")));
		return;
	}
	
	lwlc_register_gucs();

	prev_set_lwlsn_block_range_hook = set_lwlsn_block_range_hook;
	set_lwlsn_block_range_hook = neon_set_lwlsn_block_range;
	prev_set_lwlsn_block_v_hook = set_lwlsn_block_v_hook;
	set_lwlsn_block_v_hook = neon_set_lwlsn_block_v;
	prev_set_lwlsn_block_hook = set_lwlsn_block_hook;
	set_lwlsn_block_hook = neon_set_lwlsn_block;
	prev_set_max_lwlsn_hook = set_max_lwlsn_hook;
	set_max_lwlsn_hook = neon_set_max_lwlsn;
	prev_set_lwlsn_relation_hook = set_lwlsn_relation_hook;
	set_lwlsn_relation_hook = neon_set_lwlsn_relation;
	prev_set_lwlsn_db_hook = set_lwlsn_db_hook;
	set_lwlsn_db_hook = neon_set_lwlsn_db;
}


void
LwLsnCacheShmemRequest(void)
{
	Size requested_size = sizeof(LwLsnCacheCtl);

	requested_size += hash_estimate_size(lwlsn_cache_size, sizeof(LastWrittenLsnCacheEntry));

	RequestAddinShmemSpace(requested_size);
}

void
LwLsnCacheShmemInit(void)
{
	static HASHCTL info;
	bool found;
	if (lwlsn_cache_size > 0)
	{
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(LastWrittenLsnCacheEntry);
		lastWrittenLsnCache = ShmemInitHash("last_written_lsn_cache",
			lwlsn_cache_size, lwlsn_cache_size,
										&info,
										HASH_ELEM | HASH_BLOBS);
		LwLsnCache = (LwLsnCacheCtl*)ShmemInitStruct("neon/LwLsnCacheCtl", sizeof(LwLsnCacheCtl), &found);
		// Now set the size in the struct
		LwLsnCache->lastWrittenLsnCacheSize = lwlsn_cache_size;
		if (found) {
			return;
		}
	}
	dlist_init(&LwLsnCache->lastWrittenLsnLRU);
    LwLsnCache->maxLastWrittenLsn = GetRedoRecPtr();
}

/*
 * neon_get_lwlsn -- Returns maximal LSN of written page.
 * It returns an upper bound for the last written LSN of a given page,
 * either from a cached last written LSN or a global maximum last written LSN.
 * If rnode is InvalidOid then we calculate maximum among all cached LSN and maxLastWrittenLsn.
 * If cache is large enough, iterating through all hash items may be rather expensive.
 * But neon_get_lwlsn(InvalidOid) is used only by neon_dbsize which is not performance critical.
 */
XLogRecPtr
neon_get_lwlsn(NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno)
{
	XLogRecPtr lsn;
	LastWrittenLsnCacheEntry* entry;

	/* Check if shared memory is initialized */
	if (LwLsnCache == NULL || LastWrittenLsnLock == NULL || 
		LwLsnCache->lastWrittenLsnCacheSize == 0 || lastWrittenLsnCache == NULL)
	{
		/*
		 * In processes where Neon lwLSN cache is not initialized (e.g. some
		 * background workers in openGauss), fall back to a conservative LSN
		 * instead of asserting in neon_get_request_lsns().
		 *
		 * We don't need an exact page LSN here; any upper bound that is not
		 * InvalidXLogRecPtr is fine for building Neon page requests.
		 */
		if (RecoveryInProgress())
			lsn = GetXLogReplayRecPtr(NULL);
		else
			lsn = GetXLogInsertRecPtr();

		if (lsn == InvalidXLogRecPtr)
			lsn = 1;	/* should not happen, but be extra defensive */

		return lsn;
	}

	LWLockAcquire(LastWrittenLsnLock, LW_SHARED);

	/* Maximal last written LSN among all non-cached pages */
	lsn = LwLsnCache->maxLastWrittenLsn;

	if (NInfoGetRelNumber(rlocator) != InvalidOid)
	{
		BufferTag key;
		Oid spcOid = NInfoGetSpcOid(rlocator);
		Oid dbOid = NInfoGetDbOid(rlocator);
		Oid relNumber = NInfoGetRelNumber(rlocator);
		BufTagInit(key,  relNumber, forknum, blkno, spcOid, dbOid);
		
		entry = (LastWrittenLsnCacheEntry*)hash_search(lastWrittenLsnCache, &key, HASH_FIND, NULL);
		if (entry != NULL)
			lsn = entry->lsn;
		else
		{
			LWLockRelease(LastWrittenLsnLock);
			LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);
			/*
			 * In case of statements CREATE TABLE AS SELECT... or INSERT FROM SELECT... we are fetching data from source table
			 * and storing it in destination table. It cause problems with prefetch last-written-lsn is known for the pages of
			 * source table (which for example happens after compute restart). In this case we get get global value of
			 * last-written-lsn which is changed frequently as far as we are writing pages of destination table.
			 * As a result request-lsn for the prefetch and request-let when this page is actually needed are different
			 * and we got exported prefetch request. So it actually disarms prefetch.
			 * To prevent that, we re-insert the page with the latest LSN, so that it's
			 * less likely the LSN for this page will get evicted from the LwLsnCache
			 * before the page is read.
			 */
			 lsn = SetLastWrittenLSNForBlockRangeInternal(lsn, rlocator, forknum, blkno, 1);
		}
	}
	else
	{
		HASH_SEQ_STATUS seq;
		/* Find maximum of all cached LSNs */
		hash_seq_init(&seq, lastWrittenLsnCache);
		while ((entry = (LastWrittenLsnCacheEntry *) hash_seq_search(&seq)) != NULL)
		{
			if (entry->lsn > lsn)
				lsn = entry->lsn;
		}
	}
	LWLockRelease(LastWrittenLsnLock);

	return lsn;
}

static void neon_set_max_lwlsn(XLogRecPtr lsn) {
	/* Check if shared memory is initialized */
	if (LwLsnCache == NULL || LastWrittenLsnLock == NULL)
		return;
	
	LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);
	LwLsnCache->maxLastWrittenLsn = lsn;
	LWLockRelease(LastWrittenLsnLock);
}

/*
 * GetLastWrittenLSN -- Returns maximal LSN of written page.
 * It returns an upper bound for the last written LSN of a given page,
 * either from a cached last written LSN or a global maximum last written LSN.
 * If rnode is InvalidOid then we calculate maximum among all cached LSN and maxLastWrittenLsn.
 * If cache is large enough, iterating through all hash items may be rather expensive.
 * But GetLastWrittenLSN(InvalidOid) is used only by neon_dbsize which is not performance critical.
 */
void
neon_get_lwlsn_v(NRelFileInfo relfilenode, ForkNumber forknum,
				   BlockNumber blkno, int nblocks, XLogRecPtr *lsns)
{
	LastWrittenLsnCacheEntry* entry;
	XLogRecPtr lsn;

	/* Check if shared memory is initialized */
	if (LwLsnCache == NULL || LastWrittenLsnLock == NULL || 
		LwLsnCache->lastWrittenLsnCacheSize == 0 || lastWrittenLsnCache == NULL)
	{
		/*
		 * In processes where Neon lwLSN cache is not initialized (e.g. some
		 * background workers in openGauss), fall back to a conservative LSN
		 * instead of returning InvalidXLogRecPtr which would trip assertions
		 * in neon_get_request_lsns().
		 *
		 * We use the current replay LSN during recovery, or the current insert
		 * LSN in normal operation, as an upper bound.
		 */
		if (RecoveryInProgress())
			lsn = GetXLogReplayRecPtr(NULL);
		else
			lsn = GetXLogInsertRecPtr();

		if (lsn == InvalidXLogRecPtr)
			lsn = 1;

		for (int i = 0; i < nblocks; i++)
			lsns[i] = lsn;
		return;
	}

	Assert(nblocks > 0);
	Assert(PointerIsValid(lsns));

	LWLockAcquire(LastWrittenLsnLock, LW_SHARED);

	if (NInfoGetRelNumber(relfilenode) != InvalidOid)
	{
		BufferTag key;
		bool missed_keys = false;
		Oid spcOid = NInfoGetSpcOid(relfilenode);
		Oid dbOid = NInfoGetDbOid(relfilenode);
		Oid relNumber = NInfoGetRelNumber(relfilenode);
		BufTagInit(key,  relNumber, forknum, blkno, spcOid, dbOid);

		for (int i = 0; i < nblocks; i++)
		{
			/* Maximal last written LSN among all non-cached pages */
			key.blockNum = blkno + i;

			entry = (LastWrittenLsnCacheEntry*)hash_search(lastWrittenLsnCache, &key, HASH_FIND, NULL);
			if (entry != NULL)
			{
 				lsns[i] = entry->lsn;
			}
			else
			{
				/* Mark this block's LSN as missing - we'll update the LwLSN for missing blocks in bulk later */
				lsns[i] = InvalidXLogRecPtr;
				missed_keys = true;
			}
		}

		/*
		 * If we had any missing LwLSN entries, we add the missing ones now.
		 * By doing the insertions in one batch, we decrease lock contention.
		 */
		if (missed_keys)
		{
			LWLockRelease(LastWrittenLsnLock);
			LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);

			lsn = LwLsnCache->maxLastWrittenLsn;

			for (int i = 0; i < nblocks; i++)
			{
				if (lsns[i] == InvalidXLogRecPtr)
				{
					lsns[i] = lsn;
					SetLastWrittenLSNForBlockRangeInternal(lsn, relfilenode, forknum, blkno + i, 1);
				}
			}
		}
	}
	else
	{
		HASH_SEQ_STATUS seq;
		lsn = LwLsnCache->maxLastWrittenLsn;
		/* Find maximum of all cached LSNs */
		hash_seq_init(&seq, lastWrittenLsnCache);
		while ((entry = (LastWrittenLsnCacheEntry *) hash_seq_search(&seq)) != NULL)
		{
			if (entry->lsn > lsn)
				lsn = entry->lsn;
		}

		for (int i = 0; i < nblocks; i++)
			lsns[i] = lsn;
	}
	LWLockRelease(LastWrittenLsnLock);
}

/*
 * Guts for SetLastWrittenLSNForBlockRange.
 * Caller must ensure LastWrittenLsnLock is held in exclusive mode.
 */
static XLogRecPtr
SetLastWrittenLSNForBlockRangeInternal(XLogRecPtr lsn,
									   NRelFileInfo rlocator,
									   ForkNumber forknum,
									   BlockNumber from,
									   BlockNumber n_blocks)
{
	if (NInfoGetRelNumber(rlocator) == InvalidOid)
	{
		if (lsn > LwLsnCache->maxLastWrittenLsn)
		LwLsnCache->maxLastWrittenLsn = lsn;
		else
			lsn = LwLsnCache->maxLastWrittenLsn;
	}
	else
	{
		LastWrittenLsnCacheEntry* entry;
		BufferTag key;
		bool found;
		BlockNumber i;

		Oid spcOid = NInfoGetSpcOid(rlocator);
		Oid dbOid = NInfoGetDbOid(rlocator);
		Oid relNumber = NInfoGetRelNumber(rlocator);
		BufTagInit(key,  relNumber, forknum, from, spcOid, dbOid);
		for (i = 0; i < n_blocks; i++)
		{
			key.blockNum = from + i;
			entry = (LastWrittenLsnCacheEntry*)hash_search(lastWrittenLsnCache, &key, HASH_ENTER, &found);
			if (found)
			{
				if (lsn > entry->lsn)
					entry->lsn = lsn;
				else
					lsn = entry->lsn;
				/* Unlink from LRU list */
				dlist_delete(&entry->lru_node);
			}
			else
			{
				entry->lsn = lsn;
				if (hash_get_num_entries(lastWrittenLsnCache) > LwLsnCache->lastWrittenLsnCacheSize)
				{
					/* Replace least recently used entry */
					LastWrittenLsnCacheEntry* victim = dlist_container(LastWrittenLsnCacheEntry, lru_node, dlist_pop_head_node(&LwLsnCache->lastWrittenLsnLRU));
					/* Adjust max LSN for not cached relations/chunks if needed */
					if (victim->lsn > LwLsnCache->maxLastWrittenLsn)
					LwLsnCache->maxLastWrittenLsn = victim->lsn;

					hash_search(lastWrittenLsnCache, victim, HASH_REMOVE, NULL);
				}
			}
			/* Link to the end of LRU list */
			dlist_push_tail(&LwLsnCache->lastWrittenLsnLRU, &entry->lru_node);
		}
	}
	return lsn;
}

/*
 * SetLastWrittenLSNForBlockRange -- Set maximal LSN of written page range.
 * We maintain cache of last written LSNs with limited size and LRU replacement
 * policy. Keeping last written LSN for each page allows to use old LSN when
 * requesting pages of unchanged or appended relations. Also it is critical for
 * efficient work of prefetch in case massive update operations (like vacuum or remove).
 *
 * rlocator.relNumber can be InvalidOid, in this case maxLastWrittenLsn is updated.
 * SetLastWrittenLsn with dummy rlocator is used by createdb and dbase_redo functions.
 */
XLogRecPtr
neon_set_lwlsn_block_range(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber from, BlockNumber n_blocks)
{
	/* Check if shared memory is initialized */
	if (LwLsnCache == NULL || LastWrittenLsnLock == NULL || 
		lastWrittenLsnCache == NULL)
		return lsn;
	
	if (lsn == InvalidXLogRecPtr || n_blocks == 0 || LwLsnCache->lastWrittenLsnCacheSize == 0)
		return lsn;

	// Assert(lsn >= WalSegMinSize);
	LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);
	lsn = SetLastWrittenLSNForBlockRangeInternal(lsn, rlocator, forknum, from, n_blocks);
	LWLockRelease(LastWrittenLsnLock);

	return lsn;
}

/*
 * neon_set_lwlsn_block_v -- Set maximal LSN of pages to their respective
 * LSNs.
 *
 * We maintain cache of last written LSNs with limited size and LRU replacement
 * policy. Keeping last written LSN for each page allows to use old LSN when
 * requesting pages of unchanged or appended relations. Also it is critical for
 * efficient work of prefetch in case massive update operations (like vacuum or remove).
 *
 * Note: This is different from SetLastWrittenLSNForBlockRange[Internal], in that this
 * specifies per-block LSNs, rather than only a single LSN.
 */
XLogRecPtr
neon_set_lwlsn_block_v(const XLogRecPtr *lsns, NRelFileInfo relfilenode,
						   ForkNumber forknum, BlockNumber blockno,
						   int nblocks)
{
	LastWrittenLsnCacheEntry* entry;
	BufferTag	key;
	bool		found;
	XLogRecPtr	max = InvalidXLogRecPtr;
	Oid spcOid = NInfoGetSpcOid(relfilenode);
	Oid dbOid = NInfoGetDbOid(relfilenode);
	Oid relNumber = NInfoGetRelNumber(relfilenode);

	/* Check if shared memory is initialized */
	if (LwLsnCache == NULL || LastWrittenLsnLock == NULL || 
		lastWrittenLsnCache == NULL)
		return InvalidXLogRecPtr;

	if (lsns == NULL || nblocks == 0 || LwLsnCache->lastWrittenLsnCacheSize == 0 ||
		NInfoGetRelNumber(relfilenode) == InvalidOid)
		return InvalidXLogRecPtr;

	BufTagInit(key,  relNumber, forknum, blockno, spcOid, dbOid);

	LWLockAcquire(LastWrittenLsnLock, LW_EXCLUSIVE);

	for (int i = 0; i < nblocks; i++)
	{
		XLogRecPtr	lsn = lsns[i];

		if (lsn == InvalidXLogRecPtr)
			continue;

		// Assert(lsn >= WalSegMinSize);
		key.blockNum = blockno + i;
		entry = (LastWrittenLsnCacheEntry*)hash_search(lastWrittenLsnCache, &key, HASH_ENTER, &found);
		if (found)
		{
			if (lsn > entry->lsn)
				entry->lsn = lsn;
			else
				lsn = entry->lsn;
			/* Unlink from LRU list */
			dlist_delete(&entry->lru_node);
		}
		else
		{
			entry->lsn = lsn;
			if (hash_get_num_entries(lastWrittenLsnCache) > LwLsnCache->lastWrittenLsnCacheSize)
			{
				/* Replace least recently used entry */
				LastWrittenLsnCacheEntry* victim = dlist_container(LastWrittenLsnCacheEntry, lru_node, dlist_pop_head_node(&LwLsnCache->lastWrittenLsnLRU));
				/* Adjust max LSN for not cached relations/chunks if needed */
				if (victim->lsn > LwLsnCache->maxLastWrittenLsn)
					LwLsnCache->maxLastWrittenLsn = victim->lsn;

				hash_search(lastWrittenLsnCache, victim, HASH_REMOVE, NULL);
			}
		}
		/* Link to the end of LRU list */
		dlist_push_tail(&LwLsnCache->lastWrittenLsnLRU, &entry->lru_node);
		max = Max(max, lsn);
	}

	LWLockRelease(LastWrittenLsnLock);

	return max;
}

/*
 * SetLastWrittenLSNForBlock -- Set maximal LSN for block
 */
XLogRecPtr
neon_set_lwlsn_block(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno)
{
	return neon_set_lwlsn_block_range(lsn, rlocator, forknum, blkno, 1);
}

/*
 * neon_set_lwlsn_relation -- Set maximal LSN for relation metadata
 */
XLogRecPtr
neon_set_lwlsn_relation(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum)
{
	//return neon_set_lwlsn_block(lsn, rlocator, forknum, REL_METADATA_PSEUDO_BLOCKNO);
	return neon_set_lwlsn_block(lsn, rlocator, forknum, InvalidBlockNumber);
}

/*
 * neon_set_lwlsn_db -- Set maximal LSN for the whole database
 */
XLogRecPtr
neon_set_lwlsn_db(XLogRecPtr lsn)
{
	NRelFileInfo dummyNode = {InvalidOid, InvalidOid, InvalidOid};
	return neon_set_lwlsn_block(lsn, dummyNode, MAIN_FORKNUM, 0);
}

