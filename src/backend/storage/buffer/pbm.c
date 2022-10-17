/*
 * Predictive Buffer Manager
 */
#include "postgres.h"

/* PBM includes */
#include "storage/pbm.h"
#include "storage/pbm/pbm_background.h"
#include "storage/pbm/pbm_internal.h"

/* Other files */
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"

// included last to avoid IDE complaining about unused imports...
#include "storage/buf_internals.h"
#include "access/heapam.h"
#include "catalog/index.h"
#include "lib/simplehash.h"

#include <time.h>

// TODO! look for ### comments --- low-priority/later TODOs
// TODO! look for DEBUGGING comments and disable/remove them once they definitely aren't needed



/* Global pointer to the single PBM */
PbmShared* pbm;


/*-------------------------------------------------------------------------
 * Prototypes for private methods
 *-------------------------------------------------------------------------
 */


// Shared logic of the public API methods (register/unregister/report position)
static BlockGroupHashEntry * RegisterInitBlockGroupEntries(BlockGroupHashKey * bgkey, BlockNumber nblock_segs);
static void UnregisterDeleteScan(ScanId id, SharedScanStats stats);


// get current time
static inline unsigned long get_time_ns(void);
static inline unsigned long get_timeslice(void);


// initialization for internal structs
static inline void InitSeqScanStatsEntry(BlockGroupScanListElem * temp, ScanId id, ScanHashEntry * sdata, BlockNumber bgnum);
static inline void InitBlockGroupData(BlockGroupData * data);


// memory management for BlockGroupScanListElem
static inline BlockGroupScanListElem * try_get_bg_scan_elem(void);
static inline void free_bg_scan_elem(BlockGroupScanListElem *it);


// lookups in applicable hash maps
static inline ScanHashEntry * search_scan(ScanId id, HASHACTION action, bool* foundPtr);
static inline BlockGroupData * search_block_group(const BufferDesc * buf, bool* foundPtr);

static BlockGroupData * search_or_create_block_group(const BufferDesc * buf);


// managing buffer <--> block group links
// this is most of the real work for the callbacks from freelist.c
static inline BlockGroupData * AddBufToBlockGroup(BufferDesc * buf);
static inline void RemoveBufFromBlockGroup(BufferDesc * buf);


// managing buffer priority
static inline unsigned long ScanTimeToNextConsumption(const BlockGroupScanListElem * bg_scan);

static unsigned long PageNextConsumption(BlockGroupData * bgdata, bool * requestedPtr);


// removing scans from block groups
static inline void remove_scan_from_block_range(BlockGroupHashKey *bs_key, ScanId id, uint32 lo, uint32 hi);
static inline bool block_group_delete_scan(ScanId id, BlockGroupData * groupData);


// PQ methods
static inline void RefreshBlockGroup(BlockGroupData * data);
static inline void PQ_RefreshRequestedBuckets(void);


// debugging
// ### clean this up eventually
#ifdef TRACE_PBM_PRINT_SCANMAP
static void debug_append_scan_data(StringInfoData* str, ScanHashEntry* entry);
static void debug_log_scan_map(void);
#endif

static void debug_buffer_access(BufferDesc* buf, char* msg);

#ifdef SANITY_PBM_BUFFERS
static void list_all_buffers(void);

// sanity checks
static void sanity_check_verify_block_group_buffers(const BufferDesc * buf);
#endif


/*-------------------------------------------------------------------------
 *  PBM initialization methods
 *-------------------------------------------------------------------------
 */

/*
 * Initialization of shared PBM data structures
 */
void InitPBM(void) {
	bool found;
	int hash_flags;
	HASHCTL hash_info;
	struct timespec ts;

	/* Create shared PBM struct */
	pbm = (PbmShared*) ShmemInitStruct("Predictive buffer manager", sizeof(PbmShared), &found);

	/* If the PBM was already initialized, nothing to do. */
	if (true == found) {
		Assert(IsUnderPostmaster);
		return;
	}

	/* Otherwise, ensure the PBM is only initialized in the postmaster */
	Assert(!IsUnderPostmaster);

	/* Initialize fields */
	pbm->next_id = 0;
	SpinLockInit(&pbm->scan_free_list_lock);
	slist_init(&pbm->bg_scan_free_list);
	pbm->initial_est_speed = 0.0001f;
// ### what should be initial-initial speed estimate lol
// ### need to adjust it for units...

	/* Record starting time */
	clock_gettime(PBM_CLOCK, &ts);
	pbm->start_time_sec = ts.tv_sec;

	/* Initialize map of scans */
	hash_info = (HASHCTL){
		.keysize = sizeof(ScanId),
		.entrysize = sizeof(ScanHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->ScanMap = ShmemInitHash("PBM active scan stats", 128, ScanMapMaxSize, &hash_info, hash_flags);

// ### make this partitioned! (HASH_PARTITION)
	/* Initialize map of block groups */
	hash_info = (HASHCTL) {
		.keysize = sizeof(BlockGroupHashKey),
		.entrysize = sizeof(BlockGroupHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->BlockGroupMap = ShmemInitHash("PBM block group stats", 1024, BlockGroupMapMaxSize, &hash_info, hash_flags);

	/* Initialize the priority queue */
	pbm->BlockQueue = InitPbmPQ();
}

/*
 * Estimate size of PBM (including all shared structures)
 */
Size PbmShmemSize(void) {
	Size size = 0;
	size = add_size(size, sizeof(PbmShared));
	size = add_size(size, hash_estimate_size(ScanMapMaxSize, sizeof(ScanHashEntry)));
	size = add_size(size, hash_estimate_size(BlockGroupMapMaxSize, sizeof(BlockGroupHashEntry)));

// ### total size estimate of list of scans on each block group
//	size = add_size(size, sizeof(BlockGroupScanListElem) * ???)
	size = add_size(size, PbmPqShmemSize());


	// actually estimate the size later... for now assume 100 MiB will be enough
	size = add_size(size, 100 << 6);
	return size;
}


/*-------------------------------------------------------------------------
 * Public API: Sequential scan methods
 *-------------------------------------------------------------------------
 */

/*
 * Setup data structures for a new sequential scan.
 */
void PBM_RegisterSeqScan(HeapScanDesc scan) {
	bool found;
	ScanId id;
	ScanHashEntry * s_entry;
	TableData tbl;
	BlockGroupHashKey bgkey;
	BlockGroupHashEntry * bseg_first;
	BlockNumber startblock;
	BlockNumber nblocks, nblock_groups, nblock_segs;
	BlockNumber last_block, last_block_group, last_block_seg;
	BlockGroupScanListElem * new_stats = NULL;
	int bgnum;
	const float init_est_speed = pbm->initial_est_speed;

	/*
	 * Get stats from the scan.
	 *
	 * Parallel scans need special handling. We make sure to not *crash* or
	 * cause UB here, but note that parallel scans are not really supported
	 * right now...
	 */
	if (scan->rs_base.rs_parallel != NULL) {
		/* Get fields from the parallel scan data if applicable */
		ParallelBlockTableScanDesc pscan = (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;

		startblock	= pscan->phs_startblock;
		nblocks		= pscan->phs_nblocks;
	} else {
		/* Non-parallel scan */
		startblock	= scan->rs_startblock;
		nblocks		= scan->rs_nblocks;
	}

	/* Sanity checks */
	Assert(startblock != InvalidBlockNumber);

	/* Compute ranges */
	last_block 			= (nblocks == 0 ? 0 : nblocks - 1);
	last_block_group	= BLOCK_GROUP(last_block);
	last_block_seg		= BLOCK_GROUP_SEGMENT(last_block_group);
	nblock_groups 		= (nblocks == 0 ? 0 : last_block_group + 1);
	nblock_segs 		= (nblocks == 0 ? 0 : last_block_seg + 1);

	/* Keys for hash tables */
	tbl = (TableData){
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
	};

	bgkey = (BlockGroupHashKey) {
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
		.seg = 0,
	};


	/* Insert the scan metadata & generate scan ID */
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		// Generate scan ID
		id = pbm->next_id;
		pbm->next_id += 1;

		// Create s_entry for the scan in the hash map
		s_entry = search_scan(id, HASH_ENTER, &found);
		Assert(!found); // should be inserted...
	}

	/*
	 * Initialize the entry. It is OK to do this outside the lock, because we
	 * haven't associated the scan with any block groups yet (so no one will by
	 * trying to access it yet.
	 */
	s_entry->data = (ScanData) {
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

	/* Make sure every block group is present in the map! */
	bseg_first = RegisterInitBlockGroupEntries(&bgkey, nblock_segs);

	/*
	 * LOCKING: once we have created the entries, we no longer need to read or
	 * write the hash map so release the lock. We will iterate through the
	 * linked entries, but the relevant pointers will never change and individual
	 * block groups have separate concurrency control.
	 */

	/*
	 * Add the scan for each block group, then insert each block group into the
	 * PQ if applicable
	 */

	// refresh the PQ first if needed
	PQ_RefreshRequestedBuckets();

	Assert(nblock_groups == 0 || NULL != bseg_first);
	Assert(nblock_groups == 0 || NULL == bseg_first->seg_prev);
	bgnum = 0;
	// Loop over block group segments
	for (BlockGroupHashEntry * bseg_cur = bseg_first; bgnum < nblock_groups; bseg_cur = bseg_cur->seg_next) {
		Assert(bseg_cur != NULL);

		// Loop over block groups within a segment
		for (int i = 0; i < BLOCK_GROUP_SEG_SIZE && bgnum < nblock_groups; ++bgnum, ++i) {
			BlockGroupData *const data = &bseg_cur->groups[i];
			BlockGroupScanListElem * scan_entry = NULL;

			/* Get an element for the block group scan list */
			if (NULL != new_stats) {
				/* We've already allocated for these block groups, ise the allocated stuff... */
				scan_entry = new_stats;
				new_stats += 1;
			} else {
				/* Try to allocate from the free list */
				scan_entry = try_get_bg_scan_elem();

				/* If nothing free, allocate enough for everything else */
				if (NULL == scan_entry) {
					new_stats = ShmemAlloc((nblock_groups - bgnum) * sizeof(BlockGroupScanListElem));
					scan_entry = new_stats;
					new_stats += 1;
				}
			}

			// Initialize the list element & push to the list
			InitSeqScanStatsEntry(scan_entry, id, s_entry, bgnum);

			// Push the scan entry to the block group list
			bg_lock_scans(data, LW_EXCLUSIVE);
			slist_push_head(&data->scans_list, &scan_entry->slist);
			bg_unlock_scans(data);

			// Refresh the block group in the PQ if applicable
			RefreshBlockGroup(data);
		}
	}

	/* Scan remembers the ID, shared stats, and local stats */
	scan->scanId = id;
	scan->pbmSharedScanData = s_entry;
	scan->pbmLocalScanStats = (LocalSeqScanStats) {
			.last_report_time = get_time_ns(),
			.last_pos = startblock,
	};


	// debugging
#ifdef TRACE_PBM
	elog(INFO, "PBM_RegisterSeqScan(%lu): name=%s, nblocks=%d, num_blocks=%d, "
			   "startblock=%u, parallel=%s, scan=%p, shared_stats=%p",
		 id,
		 scan->rs_base.rs_rd->rd_rel->relname.data,
		 nblocks, 				// # of blocks in relation
		 scan->rs_numblocks, 	// max # of blocks, probably not set yet... (i.e. -1)
		 startblock,
		 (scan->rs_base.rs_parallel != NULL ? "true" : "false"),
		 scan, s_entry
	 );

#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP
#endif // TRACE_PBM
}

/*
 * Clean up after a sequential scan finishes.
 */
void PBM_UnregisterSeqScan(HeapScanDescData *scan) {
	const ScanId id = scan->scanId;
	ScanData scanData = scan->pbmSharedScanData->data;
	BlockGroupHashKey bgkey = (BlockGroupHashKey) {
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
		.seg = 0,
	};

#ifdef TRACE_PBM
	elog(INFO, "PBM_UnregisterSeqScan(%lu)", id);
#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP
#endif // TRACE_PBM


	// Shift PQ buckets if needed
	PQ_RefreshRequestedBuckets();

	// For each block in the scan: remove it from the list of scans
	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		/* upper is the last possible block group for the scan, +1 since upper
		 * bound is exclusive */
		const uint32 upper = (scanData.nblocks > 0 ? BLOCK_GROUP(scanData.nblocks-1) + 1 : 0);
		const uint32 start = BLOCK_GROUP(scan->pbmLocalScanStats.last_pos);
		const uint32 end   = (0 == scanData.startBlock ? upper : scanData.startBlock);

		// Everything before `start` should already be removed when the scan passed that location
		// Everything from `start` (inclusive) to `end` (exclusive) needs to have the scan removed

		if (0 == scanData.nblocks) {
			// Nothing to unregister if there are no blocks at all
		} else 	if (start <= end) {
			// remove between start and end
			remove_scan_from_block_range(&bgkey, id, start, end);
		} else {
			// remove between start and end, but wrapping around when appropriate
			remove_scan_from_block_range(&bgkey, id, start, upper);
			remove_scan_from_block_range(&bgkey, id, 0, end);
		}
	}

	// Remove from the scan map
	UnregisterDeleteScan(id, scanData.stats);
}

/*
 * Update progress of a sequential scan.
 */
void PBM_ReportSeqScanPosition(struct HeapScanDescData * scan, BlockNumber pos) {
	const ScanId id = scan->scanId;
	unsigned long curTime, elapsed;
	ScanHashEntry *const entry = scan->pbmSharedScanData;
	BlockNumber blocks;
	const BlockNumber prevGroupPos	= BLOCK_GROUP(scan->pbmLocalScanStats.last_pos);
	const BlockNumber curGroupPos	= BLOCK_GROUP(pos);
	float speed;
	BlockGroupHashKey bs_key;
	SharedScanStats stats;

	Assert(entry != NULL);
	stats = entry->data.stats;

#if defined(TRACE_PBM) && defined(TRACE_PBM_REPORT_PROGRESS)
	/* If we want to trace *every* call */
	elog(LOG, "ReportSeqScanPosition(%lu), pos=%u, group=%u", id, pos, curGroupPos);
#endif

	Assert(pos != InvalidBlockNumber);

// ### how often to update stats? (see what sync scan does)

	// Only update stats periodically
	if (prevGroupPos == curGroupPos) {
		// do nothing if we haven't gotten any further
		return;
	}

#if defined(TRACE_PBM) && !defined(TRACE_PBM_REPORT_PROGRESS)
	/* Only trace calls which don't return immediately */
	elog(LOG, "PBM_ReportSeqScanPosition(%lu), pos=%u, group=%u", id, pos, curGroupPos);
#endif

	Assert(entry != NULL);

	bs_key = (BlockGroupHashKey) {
		.rnode = entry->data.tbl.rnode,
		.forkNum = entry->data.tbl.forkNum,
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
		blocks = pos + entry->data.nblocks - scan->pbmLocalScanStats.last_pos;
	}
	speed = (float)(blocks) / (float)(elapsed);
// ### estimating speed: should do better than this. e.g. exponentially weighted average, or moving average.
	scan->pbmLocalScanStats.last_report_time = curTime;
	scan->pbmLocalScanStats.last_pos = pos;
	// update shared stats with a single assignment
	stats.blocks_scanned += blocks;
	stats.est_speed = speed;
	entry->data.stats = stats;


	PQ_RefreshRequestedBuckets();

	// Remove the scan from blocks in range [prevGroupPos, curGroupPos)
	if (curGroupPos != prevGroupPos) {
		LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
			BlockNumber upper = curGroupPos;

			// special handling if we wrapped around (note: if we update every block group this probably does nothing
			if (curGroupPos < prevGroupPos) {
				/* First delete the start part */
				remove_scan_from_block_range(&bs_key, id, 0, curGroupPos);

				/* find last block:
				 * (nblocks - 1) is block number of the last block, and range is [lo,hi) so add 1 to include  the last block.
				 */
				upper = BLOCK_GROUP(entry->data.nblocks - 1) + 1;
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


/*-------------------------------------------------------------------------
 * Public API: BRIN methods
 *-------------------------------------------------------------------------
 */

/*
 * Setup data structures for tracking a bitmap scan.
 */
extern void PBM_RegisterBitmapScan(struct BitmapHeapScanState * scan) {
	bool found;
	ScanId id;
	ScanHashEntry * s_entry;
	TableData tbl;
	BlockGroupHashKey bgkey;
	BlockGroupHashEntry * bseg_first;
	BlockNumber nblocks, nblock_groups, nblock_segs;
	BlockNumber last_block, last_block_group, last_block_seg;
	Relation rel = scan->ss.ss_currentRelation;

	const float init_est_speed = pbm->initial_est_speed;

	// Need to know # of blocks.
	nblocks = RelationGetNumberOfBlocks(scan->ss.ss_currentRelation);

	/* Compute ranges */
	last_block 			= (nblocks == 0 ? 0 : nblocks - 1);
	last_block_group	= BLOCK_GROUP(last_block);
	last_block_seg		= BLOCK_GROUP_SEGMENT(last_block_group);
	nblock_groups 		= (nblocks == 0 ? 0 : last_block_group + 1);
	nblock_segs 		= (nblocks == 0 ? 0 : last_block_seg + 1);

	/*
	 * TESTING
	 */
	elog(INFO, "PBM_RegisterBitmapScan!");

	{
		RelFileNode rnode1 = scan->ss.ss_currentRelation->rd_node;
		RelFileNode rnode2 = scan->ss.ss_currentScanDesc->rs_rd->rd_node;

		elog(INFO, "PBM_RegisterBitmapScan: nblocks=%d, "
				   "rnode1={spc=%u, db=%u, rel=%u}, "
				   "rnode2={spc=%u, db=%u, rel=%u}",
			 nblocks,
			 rnode1.spcNode, rnode1.dbNode, rnode1.relNode,
			 rnode2.spcNode, rnode2.dbNode, rnode2.relNode
		);
	}
	/*
	 * END TESTING
	 */



	/* Keys for hash tables */
	tbl = (TableData){
			.rnode = rel->rd_node,
			.forkNum = MAIN_FORKNUM, // Bitmap scans only use main fork (at least, `BitmapPrefetch` is hardcoded with MAIN_FORKNUM...)
	};


	/* Insert the scan metadata & generate scan ID */
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		// Generate scan ID
		id = pbm->next_id;
		pbm->next_id += 1;

		// Create s_entry for the scan in the hash map
		s_entry = search_scan(id, HASH_ENTER, &found);
		Assert(!found); // should be inserted...
	}

	/*
	 * Initialize the entry. It is OK to do this outside the lock, because we
	 * haven't associated the scan with any block groups yet (so no one will by
	 * trying to access it yet.
	 */
	s_entry->data = (ScanData) {
			// These fields never change
			.tbl = tbl,
			.startBlock = 0, // bitmap scan always starts at 0
			.nblocks = nblocks,
			// These stats will be updated later
			.stats = (SharedScanStats) {
				.est_speed = init_est_speed,
				.blocks_scanned = 0,
			},
	};

// TODO maybe move this after we find the list of block groups...
	/* Remember the PBM data in the scan */
	scan->scanId = id;
	scan->pbmSharedScanData = s_entry;

	/* Make sure every block group is present in the map! */
	bseg_first = RegisterInitBlockGroupEntries(&bgkey, nblock_segs);

	/* refresh the PQ first if needed */
	PQ_RefreshRequestedBuckets();

	// TODO add scan for the block group segments... Need to figure out exactly which block groups actually need it though!
}

/*
 * Clean up after a bitmap scan finishes.
 */
extern void PBM_UnregisterBitmapScan(struct BitmapHeapScanState * scan, char* msg) {
	const ScanId id = scan->scanId;
	ScanData scanData;
	BlockGroupHashKey bgkey = (BlockGroupHashKey) {
			.rnode = scan->ss.ss_currentRelation->rd_node,
			.forkNum = MAIN_FORKNUM, // Bitmap scans only use main fork
			.seg = 0,
	};

	/*
	 * TESTING
	 */
	// TODO remove `msg` parameter once we decide where this should get called
	elog(INFO, "PBM_UnregisterBitmapScan! %s   do_anything=%s",
		 msg, (scan->pbmSharedScanData != NULL ? "true" : "false"));
	/*
	 * END TESTING
	 */


	if (NULL == scan->pbmSharedScanData) {
		// No shared scan, do nothing
		// ### could assert there is no scan? maybe expensive
		return;
	}

	scanData = scan->pbmSharedScanData->data;

	// Shift PQ buckets if needed
	PQ_RefreshRequestedBuckets();

	// Remove from the remaining block groups
	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		const uint32 upper = (scanData.nblocks > 0 ? BLOCK_GROUP(scanData.nblocks-1) + 1 : 0);
		const uint32 start = 0; // TODO pick start from stats
		const uint32 end = upper; // TODO remember what the end should be on Register??

		// TODO improve this
		remove_scan_from_block_range(&bgkey, id, start, end);
	}

	// Remove from the scan map
	UnregisterDeleteScan(id, scanData.stats);

	// After deleting the scan, unlink from the scan state so it doesn't try to end the scan again
	scan->pbmSharedScanData = NULL;
}

/*
 * Update progress of a bitmap scan.
 */
extern void PBM_ReportBitmapScanPosition(struct BitmapHeapScanState * scan /*TODO other args?*/) {
	elog(INFO, "PBM_ReportBitmapScanPosition!   do_anything=%s",
		 (scan->pbmSharedScanData != NULL ? "true" : "false"));

	// TODO!
}


/*-------------------------------------------------------------------------
 * Public API: Tracking buffers
 *-------------------------------------------------------------------------
 */

/*
 * Notify the PBM about a new buffer so it can be added to the priority queue
 */
void PbmNewBuffer(BufferDesc * const buf) {
	BlockGroupData* group;

#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	elog(WARNING, "PbmNewBuffer added new buffer:" //"\n"
			   "\tnew={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u (%u) %d/%d}",
		 buf->buf_id, buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode, buf->tag.blockNum,
		 BLOCK_GROUP(buf->tag.blockNum), buf->pbm_bgroup_next, buf->pbm_bgroup_prev
	);
	debug_buffer_access(buf, "new buffer");
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_NEW

	// Buffer must not already be in a block group if it is new!
	Assert(FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_prev);
	Assert(FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_next);

	group = AddBufToBlockGroup(buf);

	// There must be a group -- either it already existed or we created it.
	Assert(group != NULL);
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

/*
 * ### Consider doing this unconditionally.
 * Pros: might get better estimates with more frequent updates
 * Const: for seq scans: we're refreshing several times in a row uselessly, more overhead.
 */
	// Push the bucket to the PQ if it isn't already there
	if (NULL == group->pq_bucket) {
		RefreshBlockGroup(group);
	}
}

/*
 * Notify the PBM when we *remove* a buffer to keep data structure up to date.
 */
void PbmOnEvictBuffer(struct BufferDesc *const buf) {
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

	RemoveBufFromBlockGroup(buf);
}


/*-------------------------------------------------------------------------
 * Public API: Maintenance methods called in the background
 *-------------------------------------------------------------------------
 */

/*
 * Shift buckets in the PBM PQ as necessary IF the lock can be acquired without
 * waiting.
 * If someone else is actively using the queue for anything, then do nothing.
 */
void PBM_TryRefreshRequestedBuckets(void) {
	unsigned long ts = get_timeslice();
	unsigned long last_shifted_ts = pbm->BlockQueue->last_shifted_time_slice;
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


/*-------------------------------------------------------------------------
 * Private helpers:
 *-------------------------------------------------------------------------
 */

/*
 * The main logic for Register*Scan to create block group entries if necessary.
 */
BlockGroupHashEntry * RegisterInitBlockGroupEntries(BlockGroupHashKey * bgkey, BlockNumber nblock_segs) {
	bool found;
	BlockGroupHashEntry * bseg_first = NULL;
	BlockGroupHashEntry * bseg_prev = NULL;
	BlockGroupHashEntry * bseg_cur;

	/* Make sure every block group is present in the map! */
	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
		// For each segment, create entry in the buffer map if it doesn't exist already

		for (BlockNumber seg = 0; seg < nblock_segs; ++seg) {
			bgkey->seg = seg;

			// Look up the next segment (or create it) in the hash table if necessary
			if (NULL == bseg_prev || NULL == bseg_prev->seg_next) {
				bseg_cur = hash_search(pbm->BlockGroupMap, bgkey, HASH_ENTER, &found);

				// if created a new entry, initialize it!
				if (!found) {
					bseg_cur->seg_next = NULL;
					bseg_cur->seg_prev = bseg_prev;
					for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
						/* LOCKING: the new entry is protected by PbmBlocksLock,
						 * not accessible without the hash table until we link
						 * to the previous entry.
						 */
						InitBlockGroupData(&bseg_cur->groups[i]);
					}
				}

				/*
				 * Link the previous segment to the current one.
				 *
				 * Locking: Do this AFTER initializing all the block groups!
				 * So that someone else traversing the in-order links can't
				 * find this before the block groups are initialized. (someone
				 * searching the map won't find it, because we have an exclusive
				 * lock on the map)
				 */
				if (bseg_prev != NULL) {
					// the links should only be initialized once, ever
					Assert(bseg_prev->seg_next == NULL);
					Assert(!found || bseg_cur->seg_prev == NULL);

					// link to previous if applicable
					bseg_prev->seg_next = bseg_cur;
					bseg_cur->seg_prev = bseg_prev;
				} else {
					Assert(NULL == bseg_first);
					// remember the *first* segment if there was no previous
					bseg_first = bseg_cur;
				}
			} else {
				bseg_cur = bseg_prev->seg_next;
			}

			// remember current segment as previous for next iteration
			bseg_prev = bseg_cur;
		}
	} // LOCK_GUARD

	return bseg_first;
}

/*
 * Remove a scan from the scan map on Unregister*
 */
void UnregisterDeleteScan(const ScanId id, const SharedScanStats stats) {
	// Remove the scan metadata from the map & update global stats while we have the lock
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		const float alpha = 0.85f; // ### pick something else? move to configuration?
		float new_est;
		bool found;

		// Delete the scan from the map (should be found!)
		search_scan(id, HASH_REMOVE, &found);
		Assert(found);

		// Update global initial speed estimate: geometrically-weighted average
		new_est = pbm->initial_est_speed * alpha + stats.est_speed * (1.f - alpha);
		pbm->initial_est_speed = new_est;
	}
}

/* Current time in nanoseconds */
unsigned long get_time_ns(void) {
	struct timespec now;
	clock_gettime(PBM_CLOCK, &now);

	return NS_PER_SEC * (now.tv_sec - pbm->start_time_sec) + now.tv_nsec;
}

/* Current time slice for the PQ */
unsigned long get_timeslice(void) {
	return ns_to_timeslice(get_time_ns());
}

/* Initialize an entry in the list of scans for a block group */
void InitScanStatsEntry(BlockGroupScanListElem *temp, ScanId id, ScanHashEntry *sdata, BlockNumber bgnum) {
	const BlockNumber startblock = sdata->data.startBlock;
	const BlockNumber nblocks = sdata->data.nblocks;
	// convert group # -> block #
	const BlockNumber first_block_in_group = GROUP_TO_FIRST_BLOCK(bgnum);
	BlockNumber blocks_behind;

	// calculate where the block group is in the scan relative to start block
	if (first_block_in_group >= startblock) {
		// Normal case: no circular scans or we have not wrapped around yet
		blocks_behind = first_block_in_group - startblock;
	} else {
		// Circular scans: eventually we loop back to the start "before" the start block, have to adjust
		blocks_behind = first_block_in_group + nblocks - startblock;

		/*
		 * Special case: if this is the group of the start block but startblock
		 * is NOT the first in the group, then this group is both at the start
		 * and the end. For out purposes treat it as the end, since we will
		 * access it at the start immediately and it is either in cache or not,
		 * very unlikely that tracking that information would matter.
		 *
		 * (this is automatically handled by this case)
		 */
	}

	// fill in data of the new list element
	*temp = (BlockGroupScanListElem){
			.scan_id = id,
			.scan_entry = sdata,
			.blocks_behind = blocks_behind,
	};
}

/* Initialize metadata for a block group */
void InitBlockGroupData(BlockGroupData * data) {
	slist_init(&data->scans_list);
	data->buffers_head = FREENEXT_END_OF_LIST;
	data->pq_bucket = NULL;

	// Initialize locks for the block group
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLockInitialize(&data->lock, LWTRANCHE_PBM_BLOCK_GROUP);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	SpinLockInit(&data->slock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	SpinLockInit(&data->scan_lock);
	SpinLockInit(&data->buf_lock);
#endif // PBM_BG_LOCK_MODE
}

/* Pop something of the scan stats free list. Returns NULL if it is empty */
BlockGroupScanListElem * try_get_bg_scan_elem(void) {
	BlockGroupScanListElem * ret = NULL;

	if (slist_is_empty(&pbm->bg_scan_free_list)) {
		return NULL;
	}

	SpinLockAcquire(&pbm->scan_free_list_lock);
	if (!slist_is_empty(&pbm->bg_scan_free_list)) {
		slist_node * snode = slist_pop_head_node(&pbm->bg_scan_free_list);
		ret = slist_container(BlockGroupScanListElem, slist, snode);
	}
	SpinLockRelease(&pbm->scan_free_list_lock);

	return ret;
}

/* Put the given scan list element onto the free list.
 * Shared memory can't be properly "freed" so we need to reuse it ourselves */
void free_bg_scan_elem(BlockGroupScanListElem *it) {
	SpinLockAcquire(&pbm->scan_free_list_lock);
	slist_push_head(&pbm->bg_scan_free_list, &it->slist);
	SpinLockRelease(&pbm->scan_free_list_lock);
}

/*
 * Search scan map for the given scan by its ID.
 *
 * Requires PqmScansLock to already be held as exclusive for insert/delete, at least shared for reads
 */
ScanHashEntry * search_scan(const ScanId id, HASHACTION action, bool* foundPtr) {
	return ((ScanHashEntry*)hash_search(pbm->ScanMap, &id, action, foundPtr));
}

/*
 * Find the data in the PBM corresponding to the given buffer, if we are tracking it.
 *
 * This acquires PbmBlocksLock internally for the hash lookup.
 */
BlockGroupData * search_block_group(const BufferDesc *const buf, bool* foundPtr) {
	const BlockNumber blockNum = buf->tag.blockNum;
	const BlockNumber bgroup = BLOCK_GROUP(blockNum);
	const BlockGroupHashKey bgkey = (BlockGroupHashKey) {
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
			.seg = BLOCK_GROUP_SEGMENT(bgroup),
	};
	BlockGroupHashEntry * bg_entry;

	// Look up the block group
	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		bg_entry = hash_search(pbm->BlockGroupMap, &bgkey, HASH_FIND, foundPtr);
	}

	if (false == *foundPtr) {
		return NULL;
	} else {
		return &bg_entry->groups[bgroup % BLOCK_GROUP_SEG_SIZE];
	}
}

/*
 * Find the block group for the given buffer, or create it if it doesn't exist.
 *
 * This internally acquires PbmBlocksLock in the required mode when searching the hash table.
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
					InitBlockGroupData(&bg_entry->groups[i]);
				}

				// Don't link it to adjacent segments - this will get fixed lazily by sequential scans
				bg_entry->seg_next = NULL;
				bg_entry->seg_prev = NULL;
			}
		}
	}

	// Find the block group within the segment
	return &bg_entry->groups[bgroup % BLOCK_GROUP_SEG_SIZE];
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

	// Lock block group for insert
	bg_lock_buffers(group, LW_EXCLUSIVE);

	// Link the buffer into the block group chain of buffers
	group_head = group->buffers_head;
	group->buffers_head = buf->buf_id;
	buf->pbm_bgroup_next = group_head;
	buf->pbm_bgroup_prev = FREENEXT_END_OF_LIST;
	if (group_head != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(group_head)->pbm_bgroup_prev = buf->buf_id;
	}

	bg_unlock_buffers(group);

	// If this is the first buffer in the block group, caller will add it to the PQ
	return group;
}

/*
 * Remove a buffer from its block group, and if the block group is now empty
 * remove it from the PQ as well.
 *
 * Needs: `buf` should be a valid shared buffer, and therefore must already be
 * in a block somewhere.
 */
void RemoveBufFromBlockGroup(BufferDesc *const buf) {
	int next, prev;
	bool found;
	BlockGroupData * group;
	bool need_to_remove;

	/*
	 * DEBUGGING: got this error once while dropping the *last* index on the table.
	 *
	 * Not sure why.
	 *
	 * ### remove debugging check eventually.
	 */
	if (buf->pbm_bgroup_next == FREENEXT_NOT_IN_LIST || buf->pbm_bgroup_prev == FREENEXT_NOT_IN_LIST) {
		debug_buffer_access(buf, "remove_buf_from_block_group, buffer is not in a block group!");

		PBM_DEBUG_print_pbm_state();
		PBM_DEBUG_sanity_check_buffers();
	}

	// This should never be called for a buffer which isn't in the list
	Assert(buf->pbm_bgroup_next != FREENEXT_NOT_IN_LIST);
	Assert(buf->pbm_bgroup_prev != FREENEXT_NOT_IN_LIST);

	// Need to find and lock the block group before doing anything
	group = search_block_group(buf, &found);
	Assert(found);
	bg_lock_buffers(group, LW_EXCLUSIVE);

	next = buf->pbm_bgroup_next;
	prev = buf->pbm_bgroup_prev;

	// Unlink from neighbours
	buf->pbm_bgroup_prev = FREENEXT_NOT_IN_LIST;
	buf->pbm_bgroup_next = FREENEXT_NOT_IN_LIST;

	// unlink first if needed
	if (next != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(next)->pbm_bgroup_prev = prev;
	}
	if (prev != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(prev)->pbm_bgroup_next = next;
	} else {
		// This is the first one in the list, remove from the group!
		group->buffers_head = next;
	}

	// check if the group is empty while we still have the lock
	need_to_remove = (group->buffers_head == FREENEXT_END_OF_LIST);

	bg_unlock_buffers(group);

	// If the whole list is empty now, remove the block from the PQ bucket as well
	if (need_to_remove) {
		PQ_RemoveBlockGroup(group);
	}
}

/*
 * Estimate when the specific scan will reach the relevant block group.
 */
unsigned long ScanTimeToNextConsumption(const BlockGroupScanListElem *const bg_scan) {
	ScanHashEntry * s_data = bg_scan->scan_entry;
	const BlockNumber blocks_behind = GROUP_TO_FIRST_BLOCK(bg_scan->blocks_behind);
	BlockNumber blocks_remaining;
	SharedScanStats stats;
	long res;

	// read stats from the struct
	stats = s_data->data.stats;

	// Estimate time to next access time
	// First: estimate distance (# blocks) to the block based on # of blocks scanned and position
	// 		of the block group in the scan
	// Then: distance/speed = time to next access (estimate)
	if (blocks_behind < stats.blocks_scanned) {
		// ### consider: if we've already been passed, then maybe this is "not requested" anymore?
		blocks_remaining = 0;
	} else {
		blocks_remaining = blocks_behind - stats.blocks_scanned;
	}

	res = (long)((float)blocks_remaining / stats.est_speed);
	return res;
}

/*
 * Estimate the next access time of a block group based on the tracked metadata.
 */
unsigned long PageNextConsumption(BlockGroupData *const bgdata, bool *requestedPtr) {
	unsigned long min_next_access = AccessTimeNotRequested;
	slist_iter iter;

	Assert(bgdata != NULL);

	// Initially assume not requested
	*requestedPtr = false;

	// Lock the scan list before we start
	bg_lock_scans(bgdata, LW_SHARED);

	// not requested if there are no scans
	if (slist_is_empty(&bgdata->scans_list)) {
		bg_unlock_scans(bgdata);
		return AccessTimeNotRequested;
	}

	// loop over the scans and check the next access time estimate from that scan
	slist_foreach(iter, &bgdata->scans_list) {
		BlockGroupScanListElem * it = slist_container(BlockGroupScanListElem, slist, iter.cur);

		const unsigned long time_to_next_access = ScanTimeToNextConsumption(it);

		if (time_to_next_access != AccessTimeNotRequested
				&& time_to_next_access < min_next_access) {
			min_next_access = time_to_next_access;
			*requestedPtr = true;
		}
	}

	bg_unlock_scans(bgdata);

	// return the soonest next access time if applicable
	if (false == *requestedPtr) {
		return AccessTimeNotRequested;
	} else {
		Assert(min_next_access != AccessTimeNotRequested);
		return get_time_ns() + min_next_access;
	}
}

/* Delete a specific scan from the list of the given block group */
bool block_group_delete_scan(ScanId id, BlockGroupData *groupData) {
	slist_mutable_iter iter;

	// Lock the list before we start
	bg_lock_scans(groupData, LW_EXCLUSIVE);

	// Search the list to remove the scan
	slist_foreach_modify(iter, &groupData->scans_list) {
		BlockGroupScanListElem * it = slist_container(BlockGroupScanListElem, slist, iter.cur);

		// If we find the scan: remove it and add to free list, and then we are done
		if (it->scan_id == id) {
			slist_delete_current(&iter);

			bg_unlock_scans(groupData);
			free_bg_scan_elem(it);
			return true;
		}
	}

	// Didn't find it
	bg_unlock_scans(groupData);
	return false;
}

/*
 * When the given scan is done, remove it from the remaining range of blocks.
 *
 * This requires at least SHARED lock on PbmBlocksLock, and locks the individual
 * block groups as required.
 */
void remove_scan_from_block_range(BlockGroupHashKey *bs_key, const ScanId id, const uint32 lo, const uint32 hi) {
	uint32 bgnum;
	uint32 i;
	bool found;
	BlockGroupHashEntry * bs_entry;

	// Nothing to do with empty range
	if (lo == hi) {
		return;
	}

	// Search for the starting hash entry
	bs_key->seg = BLOCK_GROUP_SEGMENT(lo);
	bs_entry = hash_search(pbm->BlockGroupMap, bs_key, HASH_FIND, &found);

	/*
	 * DEBUGGING: this got an error during `create index`
	 */
	if (!found) {
		elog(LOG, "remove_scan_from_block_range(id=%lu, lo=%u, hi=%u) key={rnode={spc=%u, db=%u, rel=%u}, fork=%d, seg=%u}",
 			 id, lo, hi,
			 bs_key->rnode.spcNode, bs_key->rnode.dbNode, bs_key->rnode.relNode,
			 bs_key->forkNum, bs_key->seg
		);

		PBM_DEBUG_print_pbm_state();
		PBM_DEBUG_sanity_check_buffers();
	}

	/* Sanity checks */
	Assert(lo <= hi);
	Assert(found);

	// Loop over the linked hash map entries
	bgnum 	= lo;
	i 		= bgnum % BLOCK_GROUP_SEG_SIZE;
	for ( ; bs_entry != NULL && bgnum < hi; bs_entry = bs_entry->seg_next) {
		// Loop over block groups in the entry
		for ( ; i < BLOCK_GROUP_SEG_SIZE && bgnum < hi; ++i, ++bgnum) {
			BlockGroupData * block_group = &bs_entry->groups[i];
			bool deleted = block_group_delete_scan(id, block_group);
			if (deleted) {
				RefreshBlockGroup(block_group);
			}
		}

		// start at first block group of the next entry
		i = 0;
	}

	/*
	 * DEBUGGING: make sure we could iterate all the way through...
	 */
	if (bgnum < hi) {
		elog(WARNING, "didnt get to the end of the range! expected %u but only got to %u",
			 hi, bgnum);
	}
}

/*
 * Refresh a block group in the PQ.
 *
 * This computes the next consumption time and moves the block group to the appropriate bucket,
 * removing it from the current one first.
 */
void RefreshBlockGroup(BlockGroupData *const data) {
	bool requested;
	bool has_buffers = (data->buffers_head >= 0);
	unsigned long t;

	// Check if this group should be in the PQ.
	// If so, move it to the appropriate bucket. If not, remove it from its bucket if applicable.
	if (has_buffers) {
		t = PageNextConsumption(data, &requested);
		PQ_RefreshBlockGroup(data, t, requested);
	} else {
		PQ_RemoveBlockGroup(data);
	}
}

/*
 * If enough time has passed, shift the PQ buckets to reflect the passage of time.
 *
 * This should generally be called just before inserting or refreshing a block
 * group. We don't want to put a block group in the wrong bucket just because
 * the buckets are out of date.
 */
void PQ_RefreshRequestedBuckets(void) {
	unsigned long ts = get_timeslice();
	unsigned long last_shifted_ts = pbm->BlockQueue->last_shifted_time_slice;
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



#if PBM_EVICT_MODE == PBM_EVICT_MODE_SINGLE
BufferDesc* PBM_EvictPage(uint32 * buf_state) {
	return PQ_Evict(pbm->BlockQueue, buf_state);
}
#endif // PBM_EVICT_MODE


// ### clean up the debugging code below

#ifdef SANITY_PBM_BUFFERS
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
#endif // SANITY_PBM_BUFFERS

void debug_buffer_access(BufferDesc* buf, char* msg) {
	bool found, requested;
	char* msg2;
	BlockNumber blockNum = buf->tag.blockNum;
	BlockNumber blockGroup = BLOCK_GROUP(blockNum);
	TableData tbl = (TableData){
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
	};
	unsigned long next_access_time = AccessTimeNotRequested;
	unsigned long now = get_time_ns();

	BlockGroupData *const block_scans = search_block_group(buf, &found);
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

#ifdef TRACE_PBM_PRINT_SCANMAP
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
#endif

