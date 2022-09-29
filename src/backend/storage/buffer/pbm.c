/*
 * Predictive Buffer Manager
 */
#include "postgres.h"

#include "storage/pbm.h"
#include "storage/pbm/pbm_background.h"
#include "storage/pbm/pbm_internal.h"

#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"

// included last to avoid IDE complaining about unused imports...
#include "storage/buf_internals.h"
#include "access/heapam.h"

#include <time.h>


// TODO! look for ### comments --- low-priority/later TODOs




///-------------------------------------------------------------------------
/// Structs and typedefs
///-------------------------------------------------------------------------




/// Global pointer to the single PBM
PbmShared* pbm;


///-------------------------------------------------------------------------
/// Prototypes for private methods
///-------------------------------------------------------------------------

// lookup in applicable hash maps
static inline ScanData * search_scan(ScanId id, HASHACTION action, bool* foundPtr);
static BlockGroupData * search_block_group(const BufferDesc * buf, bool* foundPtr);
static BlockGroupData * search_or_create_block_group(const BufferDesc * buf);

// various helpers to improve code reuse and readability
static void InitScanStatsEntry(BlockGroupScanList* temp, ScanId id, ScanData* sdata, BlockGroupScanList* next, BlockNumber bgnum);
static bool DeleteScanFromGroup(ScanId id, BlockGroupData* groupData);
static BlockGroupData * AddBufToBlockGroup(BufferDesc * buf);

// debugging
static void debug_append_scan_data(StringInfoData* str, ScanHashEntry* entry);
static void debug_log_scan_map(void);
//static void debug_append_buffer_data(StringInfoData * str, BlockGroupHashEntry * entry);
//static void debug_log_buffers_map(void);
static void debug_buffer_access(BufferDesc* buf, char* msg);
static void list_all_buffers(void);

// sanity checks
static void sanity_check_verify_block_group_buffers(const BufferDesc * buf);

// managing buffer priority
static long ScanTimeToNextConsumption(const BlockGroupScanList* stats, bool* foundPtr);
static long PageNextConsumption(const BlockGroupData* stats, bool *requestedPtr);


/// Inline private helpers
// ### revisit to see which other ones should be inline

static inline void RemoveBufFromBlock(BufferDesc *buf);
static inline BlockGroupData EmptyBlockGroupData(void);
static inline void RefreshBlockGroup(BlockGroupData * data);
static inline void PQ_RefreshRequestedBuckets(void);
static inline void remove_scan_from_block_range(BlockGroupHashKey * bs_key, ScanId id, uint32 lo, uint32 hi);

///-------------------------------------------------------------------------
/// Initialization methods
///-------------------------------------------------------------------------

///-------------------------------------------------------------------------
/// PBM PQ public API
///-------------------------------------------------------------------------

// Initialization of shared data structures
void InitPBM(void) {
	bool found;
	int hash_flags;
	HASHCTL hash_info;
	struct timespec ts;

	// Initialize the PBM
	pbm = (PbmShared*) ShmemInitStruct("Predictive buffer manager", sizeof(PbmShared), &found);

	// If the PBM was already initialized, nothing to do.
	if (true == found) {
		Assert(IsUnderPostmaster);
		return;
	}

	// Otherwise, ensure the PBM is only initialized in the postmaster
	Assert(!IsUnderPostmaster);

	pbm->next_id = 0;
	pbm->block_group_stats_free_list = NULL;
	pbm->initial_est_speed = 0.0001f;
// ### what should be initial-initial speed estimate lol
// ### need to adjust it for units...

	// record starting time
	clock_gettime(PBM_CLOCK, &ts);
	pbm->start_time_sec = ts.tv_sec;

	// Initialize map of scans
	hash_info = (HASHCTL){
		.keysize = sizeof(ScanId),
		.entrysize = sizeof(ScanHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->ScanMap = ShmemInitHash("PBM active scan stats", 128, ScanMapMaxSize, &hash_info, hash_flags);

// ### should this be partitioned? (HASH_PARTITION)
	// Initialize map of block groups
	hash_info = (HASHCTL) {
		.keysize = sizeof(BlockGroupHashKey),
		.entrysize = sizeof(BlockGroupHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->BlockGroupMap = ShmemInitHash("PBM block group stats", 1024, BlockGroupMapMaxSize, &hash_info, hash_flags);


	// Priority queue
	pbm->BlockQueue = InitPbmPQ();

// ### other fields to be added...

}

// Estimate size of PBM (including all shared structures)
Size PbmShmemSize(void) {
	Size size = 0;
	size = add_size(size, sizeof(PbmShared));
	size = add_size(size, hash_estimate_size(ScanMapMaxSize, sizeof(ScanHashEntry)));
	size = add_size(size, hash_estimate_size(BlockGroupMapMaxSize, sizeof(BlockGroupHashEntry)));

// ### total size estimate for the lists of block groups
//	size = add_size(size, sizeof(BlockGroupData) * ???)
// ### total size estimate of list of scans on each block group
//	size = add_size(size, sizeof(BlockGroupScanList) * ???)
	size = add_size(size, PbmPqShmemSize());

// ### ADD SIZE ESTIMATES


	// actually estimate the size later... for now assume 100 MiB will be enough (maybe increase this :)
	size = add_size(size, 100 << 6);
	return size;
}



///-------------------------------------------------------------------------
/// Public API:
///-------------------------------------------------------------------------

void RegisterSeqScan(HeapScanDesc scan) {
	bool found;
	ScanId id;
	ScanData * s_data;
	TableData tbl;
	BlockGroupHashKey bgkey;
	BlockGroupHashEntry * bseg_cur, *bseg_prev, *bseg_first;
	const BlockNumber startblock 		= scan->rs_startblock;
	const BlockNumber nblocks 			= scan->rs_nblocks;
	const BlockNumber last_block 		= (nblocks == 0 ? 0 : nblocks - 1);
	const BlockNumber last_block_group	= BLOCK_GROUP(last_block);
	const BlockNumber last_block_seg	= BLOCK_GROUP_SEGMENT(last_block_group);
	const BlockNumber nblock_groups 	= (nblocks == 0 ? 0 : last_block_group + 1);
	const BlockNumber nblock_segs 		= (nblocks == 0 ? 0 : last_block_seg + 1);
	BlockGroupScanList * new_stats 		= NULL;
	int bgnum;
	const float init_est_speed = pbm->initial_est_speed;

	tbl = (TableData){
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
	};

	bgkey = (BlockGroupHashKey) {
			.rnode = scan->rs_base.rs_rd->rd_node,
			.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
			.seg = 0,
	};


	// Insert the scan metadata
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		// Generate scan ID
		id = pbm->next_id;
		pbm->next_id += 1;

		// Create s_entry for the scan in the hash map
		s_data = search_scan(id, HASH_ENTER, &found);
		Assert(!found); // should be inserted...
	}

	/*
	 * Initialize the entry. It is OK to do this outside the lock, because we
	 * haven't associated the scan with any block groups yet (so no one will by
	 * trying to access it yet.
	 */
	*s_data = (ScanData) {
			// These fields never change
			.tbl = tbl,
			.startBlock = startblock,
			.nblocks = nblocks,
			// These stats will be updated later
			.stats = (SharedScanStats) {
				.est_speed = init_est_speed,
				.blocks_scanned = 0,
			},
	};

	/* Scan remembers the ID, shared stats, and local stats */
	scan->scanId = id;
	scan->pbmSharedScanData = s_data;
	scan->pbmLocalScanStats = (LocalScanStats) {
		.last_report_time = get_time_ns(),
		.last_pos = startblock,
	};


	// Register the scan with each buffer
	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
		// For each segment, create entry in the buffer map if it doesn't exist already
		bseg_prev = NULL;
		bseg_first = NULL;
		bgnum = 0;
		for (BlockNumber seg = 0; seg < nblock_segs; ++seg) {
			bgkey.seg = seg;

			// Look up the next segment (or create it) in the hash table if necessary
			if (NULL == bseg_prev || NULL == bseg_prev->seg_next) {
				bseg_cur = hash_search(pbm->BlockGroupMap, &bgkey, HASH_ENTER, &found);

				if (bseg_prev != NULL) {
					// link to previous if applicable
					bseg_prev->seg_next = bseg_cur;
					bseg_cur->seg_prev = bseg_prev;
				} else {
					Assert(NULL == bseg_first);
					// remember the *first* segment if there was no previous
					bseg_first = bseg_cur;
				}

				// if a new entry, initialize it!
				if (!found) {
					bseg_cur->seg_next = NULL;
					bseg_cur->seg_prev = bseg_prev;
					for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
						bseg_cur->groups[i] = EmptyBlockGroupData();
					}
				}
			} else {
				bseg_cur = bseg_prev->seg_next;
			}

			// remember current segment as previous for next iteration
			bseg_prev = bseg_cur;


			// Add the scan for each block group in the list
			for (int i = 0; i < BLOCK_GROUP_SEG_SIZE && bgnum < nblock_groups; ++bgnum, ++i) {
				BlockGroupScanList * scan_entry;
				if (pbm->block_group_stats_free_list != NULL) {
					// pop off the free list if possible
					scan_entry = pbm->block_group_stats_free_list;
					pbm->block_group_stats_free_list = pbm->block_group_stats_free_list->next;
				} else if (new_stats != NULL) {
					// if no free list, but we already allocated, use that
					scan_entry = new_stats;
					new_stats += 1;
				} else {
					// otherwise we need to allocate enough for everything else first
					new_stats = ShmemAlloc((nblock_groups - bgnum) * sizeof(BlockGroupScanList));
					scan_entry = new_stats;
					new_stats += 1;
				}

				// Initialize the list element
				InitScanStatsEntry(scan_entry, id, s_data, bseg_cur->groups[i].scans_head, bgnum);

				// Link it in
				bseg_cur->groups[i].scans_head = scan_entry;
			}
		}

// ### calculate the PRIORITY of each block (group)!
// (actually, what do we know other than the first one is needed immediately?)
		// refresh the PQ first if needed, then insert each block into the queue
		PQ_RefreshRequestedBuckets();

		Assert(nblock_groups == 0 || NULL != bseg_first);
		Assert(nblock_groups == 0 || NULL == bseg_first->seg_prev);
		bgnum = 0;
		for (bseg_cur = bseg_first; bgnum < nblock_groups; bseg_cur = bseg_cur->seg_next) {
			Assert(bseg_cur != NULL);

			for (int i = 0; i < BLOCK_GROUP_SEG_SIZE && bgnum < nblock_groups; ++bgnum, ++i) {
				BlockGroupData *const data = &bseg_cur->groups[i];
				RefreshBlockGroup(data);
			}
		}
	} // LOCK_GUARD


	// debugging
#ifdef TRACE_PBM
	elog(INFO, "RegisterSeqScan(%lu): name=%s, num_blocks=%d",
		 id,
		 scan->rs_base.rs_rd->rd_rel->relname.data,
		 scan->rs_numblocks // max # of blocks to scan: -1 = "everything"
	 );

#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP
#endif // TRACE_PBM
}


void UnregisterSeqScan(struct HeapScanDescData *scan) {
	const ScanId id = scan->scanId;
	bool found;
	ScanData scanData;
	BlockGroupHashKey bgkey = (BlockGroupHashKey) {
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
		.seg = 0,
	};

#ifdef TRACE_PBM
	elog(INFO, "UnregisterSeqScan(%lu)", id);
#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP
#ifdef TRACE_PBM_PRINT_BLOCKMAP
	debug_log_buffers_map();
#endif // TRACE_PBM_PRINT_BLOCKMAP
#endif // TRACE_PBM


	// Remove the scan metadata from the map (grab a copy first)
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		const float alpha = 0.85f; // ### pick something else? move to configuration?
		float new_est;
		SharedScanStats stats;

		/*
		 * Delete the scan from the map.
		 *
		 * Note: the hash entry will be reused by next insert but this is safe
		 * here since we have the lock.
		 */
		scanData = *search_scan(id, HASH_REMOVE, &found);
		Assert(found);
		stats = scanData.stats;

		// Update global initial speed estimate: geometrically-weighted average
		// ### Note: we don't really care about lost updates here, could put this outside the lock (make _Atomic)
		new_est = pbm->initial_est_speed * alpha + stats.est_speed * (1 - alpha);
		pbm->initial_est_speed = new_est;
	}

	// Shift PQ buckets if needed
	PQ_RefreshRequestedBuckets();

	// If there are no blocks in the scan, nothing to unregister.
	if (scanData.nblocks == 0) return;

	// For each block in the scan: remove it from the list of scans
	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
		const uint32 upper = BLOCK_GROUP(scanData.nblocks);
		const uint32 start = scan->pbmLocalScanStats.last_pos;
		const uint32 end   = (0 == scanData.startBlock ? upper : scanData.startBlock);

		// Everything before `start` should already be removed when the scan passed that location
		// Everything from `start` (inclusive) to `end` (exclusive) needs to have the scan removed

		if (start <= end) {
			// remove between start and end
			remove_scan_from_block_range(&bgkey, id, start, end);
		} else {
			// remove between start and end, but wrapping around when appropriate
			remove_scan_from_block_range(&bgkey, id, start, upper);
			remove_scan_from_block_range(&bgkey, id, 0, end);
		}
	}
}

/*
 * Update progress of a sequential scan
 */
void ReportSeqScanPosition(HeapScanDescData *const scan, BlockNumber pos) {
	const ScanId id = scan->scanId;
//	bool found;
	long curTime, elapsed;
	ScanData *const entry = scan->pbmSharedScanData;
	BlockNumber blocks;
	BlockNumber prevGroupPos, curGroupPos;
	float speed;
	BlockGroupHashKey bs_key;
	SharedScanStats stats = entry->stats;

#ifdef TRACE_PBM
	elog(LOG, "ReportSeqScanPosition (%lu)", id);
#endif

// ### how often to update stats? (see what sync scan does)

	// Only update stats periodically
	if (pos != GROUP_TO_FIRST_BLOCK(BLOCK_GROUP(pos))) {
		// do nothing in these cases
		return;
	}

	Assert(entry != NULL);

	prevGroupPos = BLOCK_GROUP(scan->pbmLocalScanStats.last_pos);
	curGroupPos = BLOCK_GROUP(pos);
	bs_key = (BlockGroupHashKey) {
		.rnode = entry->tbl.rnode,
		.forkNum = entry->tbl.forkNum,
		.seg = BLOCK_GROUP_SEGMENT(prevGroupPos),
	};

	// Note: the entry is only *written* in one process.
	// If readers aren't atomic: how bad is this? Could mis-predict next access time...
	curTime = get_time_ns();
	elapsed = curTime - scan->pbmLocalScanStats.last_report_time;
	if (pos > scan->pbmLocalScanStats.last_pos) {
		blocks = pos - scan->pbmLocalScanStats.last_pos;
	} else {
		// looped around back to the start block
		blocks = pos + entry->nblocks - scan->pbmLocalScanStats.last_pos;
	}
	speed = (float)(blocks) / (float)(elapsed);
// ### estimating speed: should do better than this. e.g. exponentially weighted average, or moving average.
	scan->pbmLocalScanStats.last_report_time = curTime;
	scan->pbmLocalScanStats.last_pos = pos;
	// update shared stats with a single assignment
	stats.blocks_scanned += blocks;
	stats.est_speed = speed;
	entry->stats = stats;


	PQ_RefreshRequestedBuckets();

	// Remove the scan from blocks in range [prevGroupPos, curGroupPos)
	if (curGroupPos != prevGroupPos) {
		LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
			BlockNumber upper = curGroupPos;

			// special handling if we wrapped around (note: if we update every block group this probably does nothing
			if (curGroupPos < prevGroupPos) {
				// First delete the start part
				remove_scan_from_block_range(&bs_key, id, 0, curGroupPos);
				// find last block
				upper = BLOCK_GROUP(entry->nblocks);
			}

			// Remove the scan from the blocks
			remove_scan_from_block_range(&bs_key, id, prevGroupPos, upper);
		}
	}

// ### consider refreshing (at least some) block groups shortly after the scan starts --- avoid case where initial speed estimate is bad.
	

#if defined(TRACE_PBM) && defined(TRACE_PBM_REPORT_PROGRESS)
	elog(INFO, "ReportSeqScanPosition(%lu) at block %d (group=%d), elapsed=%ld, blocks=%d, est_speed=%f",
		 id, pos, BLOCK_GROUP(pos), elapsed, blocks, speed );
#endif


// ### maybe want to track whether scan is forwards or backwards... (not sure if relevant)
}

// ### revisit whether to register buffers when loaded or when unpinned
// Notify the PBM about a new buffer so it can be added to the priority queue
void PbmNewBuffer(BufferDesc * const buf) {
	BlockGroupData* group;
#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	if (FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_next) {
		debug_buffer_access(buf, "new buffer");
	} else {
		debug_buffer_access(buf, "(WARNING!) new buffer already in block group, unlink first...");
	}
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_NEW

// ### make sure the buffer is not already in a block!
// TODO should be an ASSERTION here!
	RemoveBufFromBlock(buf);

#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	elog(WARNING, "PbmNewBuffer added new buffer:" //"\n"
			   "\tnew={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u (%u) %d/%d}",
		 buf->buf_id, buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode, buf->tag.blockNum,
		 BLOCK_GROUP(buf->tag.blockNum), buf->pbm_bgroup_next, buf->pbm_bgroup_prev
	);
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_NEW

	group = AddBufToBlockGroup(buf);

	// stop here if there is no group
// ### we should instead create the group!
	if (group == NULL) return;
	Assert(group->buffers_head == buf->buf_id);

#ifdef SANITY_PBM_BUFFERS
	sanity_check_verify_block_group_buffers(buf);
#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	BufferDesc* temp = GetBufferDescriptor(group->buffers_head);
	BufferDesc* temp2 = temp->pbm_bgroup_next < 0 ? NULL : GetBufferDescriptor(temp->pbm_bgroup_next);

	if (temp2 != NULL) {
		elog(INFO, "PbmNewBuffer added new buffer:"
				   "\n\t new={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}"
				   "\n\t old={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}",
			 temp->buf_id, temp->tag.rnode.spcNode, temp->tag.rnode.dbNode, temp->tag.rnode.relNode,
			 temp->tag.blockNum, BLOCK_GROUP(temp->tag.blockNum), temp->pbm_bgroup_prev, temp->pbm_bgroup_next
			 , temp2->buf_id, temp2->tag.rnode.spcNode, temp2->tag.rnode.dbNode, temp2->tag.rnode.relNode,
			 temp2->tag.blockNum, BLOCK_GROUP(temp2->tag.blockNum), temp2->pbm_bgroup_prev, temp2->pbm_bgroup_next
		);
	} else {
		elog(INFO, "PbmNewBuffer added new buffer:"
				   "\n\t new={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}"
			 	   "\n\t old={n/a}",
			 temp->buf_id, temp->tag.rnode.spcNode, temp->tag.rnode.dbNode, temp->tag.rnode.relNode,
			 temp->tag.blockNum, BLOCK_GROUP(temp->tag.blockNum), temp->pbm_bgroup_next, temp->pbm_bgroup_prev
		);
	}
#endif // tracing
#endif // SANITY_PBM_BUFFERS

	if (NULL == group->pq_bucket) {
		RefreshBlockGroup(group);
	}
}


// ### eventually want to get rid of this
inline void PbmOnEvictBuffer(struct BufferDesc *const buf) {
#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_EVICT)
	static int num_evicted = 0;
	elog(WARNING, "evicting buffer %d tbl={spc=%u, db=%u, rel=%u, fork=%d} block#=%u, #evictions=%d",
		 buf->buf_id, buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode,
		 buf->tag.forkNum, buf->tag.blockNum, num_evicted++);
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_EVICT

	// Nothing to do if we aren't actually evicting anything
	if (buf->tag.blockNum == InvalidBlockNumber) {
		return;
	}

#ifdef SANITY_PBM_BUFFERS
	// Check everything in the block group actually belongs to the same group
	sanity_check_verify_block_group_buffers(buf);
#endif // SANITY_PBM_BUFFERS

// ### need to lock the block group? (worry about it later, this needs to change anyways...)
// ### lock the PBM (shared) (?)
	RemoveBufFromBlock(buf);
}


///-------------------------------------------------------------------------
/// Private helpers:
///-------------------------------------------------------------------------

void sanity_check_verify_block_group_buffers(const BufferDesc * const buf) {
	const RelFileNode rnode = buf->tag.rnode;
	const ForkNumber  fork = buf->tag.forkNum;
	const BlockNumber bgroup = BLOCK_GROUP(buf->tag.blockNum);

	int num_traversed = 0;

	// get first buffer in the list
	const BufferDesc * it = buf;

	Assert(it->pbm_bgroup_prev > FREENEXT_NOT_IN_LIST && it->pbm_bgroup_next > FREENEXT_NOT_IN_LIST);

	while (it->pbm_bgroup_prev != FREENEXT_END_OF_LIST) {
		it = GetBufferDescriptor(it->pbm_bgroup_prev);
		++num_traversed;
		if (num_traversed % 100 == 0) {
			elog(WARNING, "sanity_check traversed 100 blocks!");

			it = buf;
			for (int i = 0; i < 10; ++i) {
				const BufferTag tag = it->tag;
				elog(WARNING, " tbl={spc=%u, db=%u, rel=%u, fork=%d} block#=%u (group=%u) prev=%d next=%d",
					 tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum, BLOCK_GROUP(tag.blockNum),
					 it->pbm_bgroup_prev, it->pbm_bgroup_next
				);
				it = GetBufferDescriptor(it->pbm_bgroup_prev);
			}

			elog(ERROR, "sanity_check traversed 100 blocks!");
			return;
		}
	}

	// make sure everything in the list has the same group
	while (it != NULL) {
		if (it->tag.rnode.spcNode != rnode.spcNode
			|| it->tag.rnode.dbNode != rnode.dbNode
			|| it->tag.rnode.relNode != rnode.relNode
			|| it->tag.forkNum != fork
			|| BLOCK_GROUP(it->tag.blockNum) != bgroup)
		{
			const BufferTag tag = it->tag;

			list_all_buffers();

			elog(ERROR, "BLOCK GROUP has buffer from the wrong group!"
						"\n\texpected: \ttbl={spc=%u, db=%u, rel=%u, fork=%d} block_group=%u"
						"\n\tgot:      \ttbl={spc=%u, db=%u, rel=%u, fork=%d} block#=%u (group=%u)",
				 rnode.spcNode, rnode.dbNode, rnode.relNode, fork, bgroup,
				 tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum, BLOCK_GROUP(tag.blockNum)
			);
		}
		if (FREENEXT_END_OF_LIST == it->pbm_bgroup_next) it = NULL;
		else it = GetBufferDescriptor(it->pbm_bgroup_next);
	}
}

void list_all_buffers(void) {
	bool found;
	BlockGroupData * data;
	Buffer bid;

	for (int i = 0; i < NBuffers; ++i) {
		BufferDesc *buf = GetBufferDescriptor(i);
		BufferTag tag = buf->tag;
		elog(WARNING, "BLOCK %d: \tspc=%u, db=%u, rel=%u, fork=%d, block=%u (group=%u) \tprev=%d  next=%d",
			 i, tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum,
			 BLOCK_GROUP(tag.blockNum), buf->pbm_bgroup_prev, buf->pbm_bgroup_next
		);

		data = search_block_group(buf, &found);

		if (!found || data == NULL) {
			elog(WARNING, "\tGROUP NOT FOUND!  prev=%d  next=%d", buf->pbm_bgroup_prev, buf->pbm_bgroup_next);
			continue;
		}

		for (bid = data->buffers_head; bid >= 0 && bid <= NBuffers; bid = GetBufferDescriptor(bid)->pbm_bgroup_next) {
			BufferDesc *buf2 = GetBufferDescriptor(bid);
			tag = buf2->tag;
			elog(WARNING, "\tbid=%d:  \tspc=%u, db=%u, rel=%u, fork=%d, block=%u (group=%u) \tPREV=%d, NEXT=%d",
				 bid,
				 tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum,
				 BLOCK_GROUP(tag.blockNum), buf2->pbm_bgroup_prev, buf2->pbm_bgroup_next
			);
		}
	}
}

void debug_buffer_access(BufferDesc* buf, char* msg) {
	bool found, requested;
	char* msg2;
	BlockNumber blockNum = buf->tag.blockNum;
	BlockNumber blockGroup = BLOCK_GROUP(blockNum);
	TableData tbl = (TableData){
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
	};
	long next_access_time = AccessTimeNotRequested;
	long now = get_time_ns();

	const BlockGroupData *const block_scans = search_block_group(buf, &found);
	if (true == found) {
		next_access_time = PageNextConsumption(block_scans, &requested);
	}

	if (false == found) msg2 = "NOT TRACKED";
	else if (false == requested) msg2 = "NOT REQUESTED";
	else msg2 = "~";

	elog(INFO, "PBM %s (%s): tbl={spc=%u, db=%u, rel=%u} block=%u group=%u --- now=%lu, next_access=%lu",
		 msg,
		 msg2,
		 tbl.rnode.spcNode,
		 tbl.rnode.dbNode,
		 tbl.rnode.relNode,
		 blockNum,
		 blockGroup,
		 now,
		 next_access_time
	);
}

/*
 * Search scan map for given scan and return reference to the data only (not including key).
 *
 * Requires PqmScansLock to already be held as exclusive for insert/delete, at least shared for reads
 */
ScanData * search_scan(const ScanId id, HASHACTION action, bool* foundPtr) {
	return &((ScanHashEntry*)hash_search(pbm->ScanMap, &id, action, foundPtr))->data;
}


/*
 * Find the block group for the given buffer, or create it if it doesn't exist.
 */
BlockGroupData * search_or_create_block_group(const BufferDesc *const buf) {
	bool found;
	BlockGroupHashEntry * bg_entry;
	const BlockNumber blockNum = buf->tag.blockNum;
	const BlockNumber bgroup = BLOCK_GROUP(blockNum);
	const BlockGroupHashKey bgkey = (BlockGroupHashKey) {
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
			.seg = BLOCK_GROUP_SEGMENT(bgroup),
	};

	// Pre-compute the hash since we may do 2 lookups
	uint32 key_hash = get_hash_value(pbm->BlockGroupMap, &bgkey);

	// Search in the hash map with the lock
	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		bg_entry = hash_search_with_hash_value(pbm->BlockGroupMap, &bgkey, key_hash, HASH_FIND, &found);
	}

	// If we didn't find the entry, create one
	if (!found) {
		LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
			bg_entry = hash_search_with_hash_value(pbm->BlockGroupMap, &bgkey, key_hash, HASH_ENTER, &found);

			// There is a chance someone else created it at the same time and got to it first
			// If not, initialize the block map entry
			if (!found) {
				// Initialize the block groups
				for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
					bg_entry->groups[i] = EmptyBlockGroupData();
				}
			// ### consider whether to search for & link the adjacent segments... this currently is done by "init scan"
				bg_entry->seg_next = NULL;
				bg_entry->seg_prev = NULL;
			}
		}
	}

	// Find the block group within the segment
	return &bg_entry->groups[bgroup % BLOCK_GROUP_SEG_SIZE];
}

// Search block group map for a particular buffer returning the location in the array
BlockGroupData * search_block_group(const BufferDesc *const buf, bool* foundPtr) {
	const BlockNumber blockNum = buf->tag.blockNum;
	const BlockNumber bgroup = BLOCK_GROUP(blockNum);
	const BlockGroupHashKey bgkey = (BlockGroupHashKey) {
		.rnode = buf->tag.rnode,
		.forkNum = buf->tag.forkNum,
		.seg = BLOCK_GROUP_SEGMENT(bgroup),
	};

	// Look up the block group
	BlockGroupHashEntry * bg_entry = hash_search(pbm->BlockGroupMap, &bgkey, HASH_FIND, foundPtr);

	if (false == *foundPtr) {
		return NULL;
	} else {
		uint32 i = bgroup % BLOCK_GROUP_SEG_SIZE;
		return &bg_entry->groups[i];
	}
}

// Print debugging information for a single entry in the scans hash map.
void debug_append_scan_data(StringInfoData* str, ScanHashEntry* entry) {
	SharedScanStats stats = entry->data.stats;
	appendStringInfo(str, "{id=%lu, start=%u, nblocks=%u, speed=%f}",
					 entry->id,
					 entry->data.startBlock,
					 entry->data.nblocks,
					 stats.est_speed
	);
}

// Print the whole scans map for debugging.
void debug_log_scan_map(void) {
	StringInfoData str;
	initStringInfo(&str);

	LOCK_GUARD_V2(PbmScansLock, LW_SHARED) {
		HASH_SEQ_STATUS status;
		ScanHashEntry * entry;

		hash_seq_init(&status, pbm->ScanMap);

		for (;;) {
			entry = hash_seq_search(&status);
			if (NULL == entry) break;

			appendStringInfoString(&str, "\n\t");
			debug_append_scan_data(&str, entry);
		}
	}

	ereport(INFO, (errmsg_internal("PBM scan map:"), errdetail_internal("%s", str.data)));
	pfree(str.data);
}

#if 0
// ### this code is out of data, but potentially could be adapted later if needed
// Print an entry in the buffers hash table for debugging. Note: the entry is for a *table* and includes an array of all buffers, so skip most of them.
void debug_append_buffer_data(StringInfoData * str, BlockGroupHashEntry * entry) {
	const int skip_amt = 100;

	appendStringInfo(str, "rel=%d [", entry->key.rnode.relNode);
	for (int i = 0; i < entry->val.len; i += skip_amt) {
		BlockGroupScanList * stat = entry->val.blockGroups[i].scans_head;

		appendStringInfo(str, "%d={", i);
		while (stat != NULL) {
			appendStringInfo(str, "(scan=%lu, behind=%u), ",
							 stat->scan_id,
							 stat->blocks_behind
			);
			stat = stat->next;
		}
		appendStringInfoString(str, "}, ");
	}
	appendStringInfoChar(str, ']');
}

// Prints (part of) the buffers map for debugging.
void debug_log_buffers_map(void) {
	StringInfoData str;
	initStringInfo(&str);

	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
		HASH_SEQ_STATUS status;
		BlockGroupHashEntry * entry;

		hash_seq_init(&status, pbm->BlockGroupMap);

		for(;;) {
			entry = hash_seq_search(&status);
			if (NULL == entry) break;

			appendStringInfoString(&str, "\n\t");
			debug_append_buffer_data(&str, entry);
		}
	}

	ereport(INFO,
			(errmsg_internal("PBM buffers map:"),
					errdetail_internal("%s", str.data)));
	pfree(str.data);
}
#endif

/*
 * Estimate the next access time of a block group based on the scan progress.
 */
long ScanTimeToNextConsumption(const BlockGroupScanList *const bg_scan, bool* foundPtr) {
	const ScanId id = bg_scan->scan_id;
	ScanData * s_data;
	const BlockNumber blocks_behind = GROUP_TO_FIRST_BLOCK(bg_scan->blocks_behind);
	BlockNumber blocks_remaining;
	SharedScanStats stats;

	LOCK_GUARD_V2(PbmScansLock, LW_SHARED) {
// ### consider a local map caching the scans
		s_data = search_scan(id, HASH_FIND, foundPtr);
	}

	// Might be possible the scan has just ended
	if (false == *foundPtr) return AccessTimeNotRequested;

	// read stats from the struct
	stats = s_data->stats;

	// Estimate time to next access time
	// First: estimate distance (# blocks) to the block based on # of blocks scanned and position
	// 		of the block group in the scan
	// Then: distance/speed = time to next access (estimate)
	if (blocks_behind < stats.blocks_scanned) {
		blocks_remaining = 0;
	} else {
		blocks_remaining = blocks_behind - stats.blocks_scanned;
	}

	return (long)((float)blocks_remaining / stats.est_speed);
}

long PageNextConsumption(const BlockGroupData *const stats, bool *requestedPtr) {
	long min_next_access = -1;
	bool found = false;

// ### lock the entry in some way?

	*requestedPtr = false;

	// not requested if there are no scans
	if (NULL == stats || NULL == stats->scans_head) {
		return AccessTimeNotRequested;
	}

	// loop over the scans and check the next access time estimate from that scan
	for (const BlockGroupScanList * it = stats->scans_head; it != NULL; it = it->next) {
		const long time_to_next_access = ScanTimeToNextConsumption(it, &found);

		if (true == found && (false == *requestedPtr || time_to_next_access < min_next_access)) {
			min_next_access = time_to_next_access;
			*requestedPtr = true;
		}
	}

	// return the soonest next access time if applicable
	if (false == *requestedPtr) {
		return AccessTimeNotRequested;
	} else {
		return get_time_ns() + min_next_access;
	}
}

// Initialize an entry in the list of scans for a block group
void InitScanStatsEntry(BlockGroupScanList *const temp, ScanId id, ScanData* sdata, BlockGroupScanList *const next, const BlockNumber bgnum) {
	const BlockNumber startblock = sdata->startBlock;
	const BlockNumber nblocks = sdata->nblocks;
	// convert group # -> block #
	const BlockNumber block_num = GROUP_TO_FIRST_BLOCK(bgnum);
	BlockNumber blocks_behind;

	// calculate where the block group is in the scan relative to start block
	if (block_num >= startblock) {
		blocks_behind = block_num - startblock;
	} else {
		blocks_behind = block_num + nblocks - startblock;
	}

	// fill in data of the new list element
	*temp = (BlockGroupScanList){
			.scan_id = id,
			.blocks_behind = blocks_behind,
			.next = next,
	};
}

// Delete a specific scan from the list of the given block group
bool DeleteScanFromGroup(const ScanId id, BlockGroupData *const groupData) {
	BlockGroupScanList *it = groupData->scans_head;
	BlockGroupScanList **prev = &groupData->scans_head;

	// find the scan in the list
	while (it != NULL && it->scan_id != id) {
		prev = &it->next;
		it = it->next;
	}

	// if found: remove it and return whether it was found
	if (NULL == it) {
		return false;
	} else {
		// unlink
		*prev = it->next;
		// add to free list
		it->next = pbm->block_group_stats_free_list;
		pbm->block_group_stats_free_list = it;
		return true;
	}
}

/*
 * Link the given buffer in to the associated block group.
 * Buffer must not already be part of any group.
 *
 * Caller is responsible for pushing the block group to the PBM PQ if necessary.
 */
BlockGroupData * AddBufToBlockGroup(BufferDesc *const buf) {
	BlockGroupData * group;
	Buffer group_head;

	// Buffer must not already be in a block group
	Assert(buf->pbm_bgroup_next == FREENEXT_NOT_IN_LIST
		&& buf->pbm_bgroup_prev == FREENEXT_NOT_IN_LIST);

	// Find or create the block group
	group = search_or_create_block_group(buf);
	Assert(group != NULL);

// ### lock the block group?

	// Link the buffer into the block group chain of buffers
	group_head = group->buffers_head;
	group->buffers_head = buf->buf_id;
	buf->pbm_bgroup_next = group_head;
	buf->pbm_bgroup_prev = FREENEXT_END_OF_LIST;
	if (group_head != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(group_head)->pbm_bgroup_prev = buf->buf_id;
	}

	// If this is the first buffer in the block group, caller will add it to the PQ
	return group;
}

long get_time_ns(void) {
	struct timespec now;
	clock_gettime(PBM_CLOCK, &now);

	return NS_PER_SEC * (now.tv_sec - pbm->start_time_sec) + now.tv_nsec;
}

long get_timeslice(void) {
	return ns_to_timeslice(get_time_ns());
}

static inline
void RemoveBufFromBlock(BufferDesc *const buf) {
	int next, prev;
// ### locking needed? (lock first when calling?)
// TODO locking --- need to always lock the block group (exclusive) to remove from it

	// Nothing to do if it isn't in the list
	if (FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_next) {
		Assert(FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_prev);
		return;
	}

	next = buf->pbm_bgroup_next;
	prev = buf->pbm_bgroup_prev;
	// unlink first if needed

	if (next != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(next)->pbm_bgroup_prev = prev;
	}
	if (prev != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(prev)->pbm_bgroup_next = next;
	} else {
		// This is the first one in the list, remove from the group!
// ### some way to optimize this?? maybe using pointers instead of indexes?
		bool found;
		BlockGroupData * group = search_block_group(buf, &found);
// ### lock the group! (need to do this anyways!)
		Assert(found);
		group->buffers_head = next;

		// If the whole list is empty now, remove the block from the PQ bucket as well
		if (next == FREENEXT_END_OF_LIST) {
			PQ_RemoveBlockGroup(group);
		}
	}

	buf->pbm_bgroup_prev = FREENEXT_NOT_IN_LIST;
	buf->pbm_bgroup_next = FREENEXT_NOT_IN_LIST;
}


static inline
void remove_scan_from_block_range(BlockGroupHashKey *bs_key, const ScanId id, const uint32 lo, const uint32 hi) {
	uint32 bgnum 	= lo;
	uint32 i 		= bgnum % BLOCK_GROUP_SEG_SIZE;
	bool found;
	BlockGroupHashEntry * bs_entry;

	// Search for the starting hash entry
	bs_key->seg = BLOCK_GROUP_SEGMENT(lo);
	bs_entry = hash_search(pbm->BlockGroupMap, bs_key, HASH_FIND, &found);
	Assert(found);

	// Loop over the linked hash map entries
	for ( ; bs_entry != NULL; bs_entry = bs_entry->seg_next) {
		// Loop over block groups in the entry
		for ( ; i < BLOCK_GROUP_SEG_SIZE && bgnum < hi; ++i, ++bgnum) {
			BlockGroupData * block_group = &bs_entry->groups[i];
			bool deleted = DeleteScanFromGroup(id, block_group);
			if (deleted) {
				RefreshBlockGroup(block_group);
			}
		}

		// start at first block group of the next entry
		i = 0;
	}
}

/*
 * Refresh a block group in the PQ.
 *
 * This computes the next consumption time and moves the block group to the appropriate bucket,
 * removing it from the current one first.
 */
static inline
void RefreshBlockGroup(BlockGroupData *const data) {
	bool requested;
	bool has_buffers = (data->buffers_head >= 0);
	long t;

	// Check if this group should be in the PQ.
	// If so, move it to the appropriate bucket. If not, remove it from its bucket if applicable.
	if (has_buffers) {
		t = PageNextConsumption(data, &requested);
		PQ_RefreshBlockGroup(data, t, requested);
	} else {
		PQ_RemoveBlockGroup(data);
	}
}

static inline
BlockGroupData EmptyBlockGroupData(void) {
	return (BlockGroupData){
		.scans_head = NULL,
		.buffers_head = FREENEXT_END_OF_LIST,
		.pq_bucket = NULL,
	};
}

// TODO decide where we should run this
static inline
void PQ_RefreshRequestedBuckets(void) {
	long ts = get_timeslice();
	long last_shifted_ts = pbm->BlockQueue->last_shifted_time_slice;
	bool up_to_date = (last_shifted_ts + 1 > ts);

#if defined(TRACE_PBM) && defined(TRACE_PBM_PQ_REFRESH)
	elog(INFO, "PBM refresh buckets: t=%ld, last=%ld, up_to_date=%s",
		 ts, last_shifted_ts, up_to_date?"true":"false");
#endif // TRACE_PBM_PQ_REFRESH

	// Nothing to do if already up to date
	if (up_to_date) return;

	// Shift the PQ buckets as many times as necessary to catch up
	LOCK_GUARD_V2(PbmPqBucketsLock, LW_EXCLUSIVE) {
		while (PQ_ShiftBucketsWithLock(ts)) ;
	}
}


/// Shift buckets in the PBM PQ as necesary IF the lock can be acquired immediately.
/// If someone else is actively using the queue for anything, then do nothing.
void PBM_TryRefreshRequestedBuckets(void) {
	long ts = get_timeslice();
	long last_shifted_ts = pbm->BlockQueue->last_shifted_time_slice;
	bool up_to_date = (last_shifted_ts + 1 > ts);
	bool acquired;

#if defined(TRACE_PBM) && defined(TRACE_PBM_PQ_REFRESH)
	elog(INFO, "PBM try refresh buckets: t=%ld, last=%ld, up_to_date=%s",
		 ts, last_shifted_ts, up_to_date?"true":"false");
#endif // TRACE_PBM_PQ_REFRESH

	// Nothing to do if already up to date
	if (up_to_date) return;

	// If unable to acquire the lock, just stop here
	acquired = LWLockConditionalAcquire(PbmPqBucketsLock, LW_EXCLUSIVE);
	if (acquired) {

		// if several time slices have passed since last shift, try to short-circuit by
		// checking if the whole PQ is empty, in which case we can just update the timestamp without actually shifting anything
		if ((ts - last_shifted_ts) > 5) {
			if (PQ_CheckEmpty()) {
				pbm->BlockQueue->last_shifted_time_slice = ts;
			}
		}

		// Shift buckets until up-to-date
		while (PQ_ShiftBucketsWithLock(ts)) ;

		LWLockRelease(PbmPqBucketsLock);
	} else {
		return;
	}
}

#if PBM_EVICT_MODE == 1
BufferDesc* PBM_EvictPage(uint32 * buf_state) {
	return PQ_Evict(pbm->BlockQueue, buf_state);
}
#endif

/*
 * TODO:
 *  - ...
 *  - revisit locking/concurrency control
 *  - revisit style for consistency: CamelCase vs snake_case?
 */
