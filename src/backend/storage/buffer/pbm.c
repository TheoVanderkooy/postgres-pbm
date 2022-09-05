/*
 * Predictive Buffer Manager
 */
#include "postgres.h"

#include "storage/pbm.h"

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
/// Configuration
///-------------------------------------------------------------------------

// Output debugging messages
#define TRACE_PBM
//#define TRACE_PBM_REPORT_PROGRESS
//#define TRACE_PBM_PRINT_SCANMAP
//#define TRACE_PBM_PRINT_BLOCKMAP
//#define TRACE_PBM_BUFFERS
//#define TRACE_PBM_BUFFERS_NEW
//#define TRACE_PBM_BUFFERS_EVICT
#define SANITY_PBM_BUFFERS

// how much to shift the block # by to find its group
#define BLOCK_GROUP_SHIFT 5 // 10

static const clock_t NotRequested = 0;

// not hard limits
#define SCAN_MAP_MAX_SIZE 1024
#define BLOCK_MAP_MAX_SIZE 1024

///-------------------------------------------------------------------------
/// Helper macros
///-------------------------------------------------------------------------

/// Helper to make sure locks get freed
/// NOTE: do NOT return from inside the lock guard (or break/continue if it will leave the scope), the lock won't be released.
//#define LOCK_GUARD_V1(lock, mode, code) LWLockAcquire((lock), (mode)); {code} LWLockRelease((lock));
#define LOCK_GUARD_V2(lock, mode) \
  for (bool _c_ = true, pg_attribute_unused() _unused = LWLockAcquire((lock), (mode)); _c_; LWLockRelease((lock)), _c_ = false)

/// Block size is too small to store *every* block individually, shift the ID to reduce the # of blocks.
#define BLOCK_GROUP(block) ((block) >> BLOCK_GROUP_SHIFT)
#define GROUP_TO_FIRST_BLOCK(group) ((group) << BLOCK_GROUP_SHIFT)


///-------------------------------------------------------------------------
/// Structs and typedefs
///-------------------------------------------------------------------------


/// Structs for storing information about scans in the hash map
// Hash map key
typedef ScanId ScanTag;

// Hash map value: scan info & statistics
// Note: records *blocks* NOT block *groups*
typedef struct ScanData {
	// Scan info (does not change after being set)
	RelFileNode	rnode; // physical relation
	ForkNumber	forkNum; // fork in the relation
	BlockNumber startBlock; // where the scan started
	BlockNumber	nblocks; // # of blocks in the table

// ### add range information (later when looking at BRIN indexes)

	// Statistics
// ### Consider concurrency control for these. Only *written* from one thread, but could be read concurrently from others
	clock_t		last_report_time;
	BlockNumber	last_pos;
	BlockNumber	blocks_scanned;
	float		est_speed;
} ScanData;

// Entry (KVP) in the hash map
typedef struct ScanHashEntry {
	ScanTag		tag; // Hash key
	ScanData	data; // Value
} ScanHashEntry;


/// Structs for information about block groups
// Hash key: table identifier
typedef struct TableData {
	RelFileNode	rnode;
	ForkNumber	forkNum;
} TableData;

// Info about a scan for the block group. (linked list)
// ### make linked-list of short arrays to improve performance (less pointer chasing and less frequent allocations)
typedef struct BlockGroupScanList {
	// Info about the current scan
	ScanId		scan_id;
	BlockNumber	blocks_behind;

	// Next item in the list
	struct BlockGroupScanList* next;
} BlockGroupScanList;

// Record data for this block group.
typedef struct BlockGroupData {
// ### some kind of lock?

	// Set of scans which care about this block group
	BlockGroupScanList* scans_head;

	// Set of (### unpinned?) buffers holding data in this block group
	Buffer buffers_head;
} BlockGroupData;

// Hash value: array of info for all block groups in the relation fork
typedef struct BlockGroupDataVec {
// ### maybe want a (spin?) lock here for searching
	uint32	len; // # of block *groups* in the table
	uint32	capacity;

	// Each is a *set* (array) of scan information
	BlockGroupData*	blockGroups;
} BlockGroupDataVec;

// Entry (KVP) in the hash map
typedef struct BlockGroupHashEntry {
	TableData			key;
	BlockGroupDataVec	val;
} BlockGroupHashEntry;


/// Main PBM data structure
typedef struct PbmShared {
	// TODO more granular locking if it could improve performance.
	// ### where to use `volatile`?

	/// Atomic counter for ID generation
	/// Note: protected by PbmScansLock: could make it atomic but it is only accessed when we lock the hash table anyways.
	volatile ScanId next_id;

	/// Map[ scan ID -> scan stats ] to record progress & estimate speed
	/// Protected by PbmScansLock
	HTAB * ScanMap;

// ### Map[ range (table -> block indexes) -> scan ] --- for looking up what scans reference a particular block, maybe useful later
	//	HTAB * ScansByRange;

	/// Map[ table -> buffer -> set of scans ] + free-list of the list-nodes for tracking statistics on each buffer
	/// Protected by PbmBlocksLock
	HTAB * BufferMap;
	BlockGroupScanList* buffer_stats_free_list;
// ### if possible, free list could be protected by its own spinlock instead of using this lock
// TODO modify the map to be: (i.e. linked hash-map of block groups, stored in fixed-size buffers to ammortize allocations and reduce the size of hash map needed)
//   Map[ (table, block group >> N) -> entry ]
//   where N is some chosen constant
//   where entry =  {
//   	Array [ 1 << N ] of BlockGroupData
//		entry* next ptr
//	 }

	/// Global estimate of speed used for *new* scans
	/// Currently NOT protected, as long as write is atomic we don't really care about lost updates...
	volatile float initial_est_speed;


// ### Potential other fields:
// ...
} PbmShared;

/// Global pointer to the single PBM
PbmShared* pbm;


///-------------------------------------------------------------------------
/// Prototypes for private methods
///-------------------------------------------------------------------------

// lookup in applicable hash maps
static ScanData* search_scan(ScanId id, HASHACTION action, bool* foundPtr);
static BlockGroupData * search_block_group(const BufferDesc * buf, bool* foundPtr);

// various helpers to improve code reuse and readability
static void InitScanStatsEntry(BlockGroupScanList* temp, ScanId id, ScanData* sdata, BlockGroupScanList* next, BlockNumber bgnum);
static bool DeleteScanFromGroup(ScanId id, BlockGroupData* groupData);
static BlockGroupData * AddBufToBlock(struct BufferDesc *buf);

// debugging
static void debug_append_scan_data(StringInfoData* str, ScanHashEntry* entry);
static void debug_log_scan_map(void);
static void debug_append_buffer_data(StringInfoData * str, BlockGroupHashEntry * entry);
static void debug_log_buffers_map(void);
static void debug_buffer_access(BufferDesc* buf, char* msg);
static void list_all_buffers();

// sanity checks
static void sanity_check_verify_block_group_buffers(const BufferDesc * buf);

// managing buffer priority
static clock_t ScanTimeToNextConsumption(const BlockGroupScanList* stats, bool* foundPtr);
static clock_t PageNextConsumption(const BlockGroupData* stats, bool *requestedPtr);


/// Inline private helpers
// ### revisit to see which other ones should be inline

static inline void RemoveBufFromBlock(BufferDesc *buf);


///-------------------------------------------------------------------------
/// Initialization methods
///-------------------------------------------------------------------------

// Initialization of shared data structures
void InitPBM(void) {
#ifdef USE_PBM
	bool found;
	int hash_flags;
	HASHCTL hash_info;

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
	pbm->buffer_stats_free_list = NULL;
	pbm->initial_est_speed = 0.1f;
// ### what should be initial-initial speed estimate lol

// ### should either of these be partitioned? (HASH_PARTITION)

	// Initialize map of scans
	hash_info = (HASHCTL){
		.keysize = sizeof(ScanTag),
		.entrysize = sizeof(ScanHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->ScanMap = ShmemInitHash("PBM active scan stats", 128, 1024, &hash_info, hash_flags);

	// Initialize map of blocks
	hash_info = (HASHCTL) {
		.keysize = sizeof(TableData),
		.entrysize = sizeof(BlockGroupHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->BufferMap = ShmemInitHash("PBM buffer stats", 32, 1024, &hash_info, hash_flags);

// ### other fields to be added...

#endif // USE_PBM
}

// Estimate size of PBM (including all shared structures)
Size PbmShmemSize(void) {
	Size size = 0;
#ifdef USE_PBM
	size = add_size(size, sizeof(PbmShared));
	size = add_size(size, hash_estimate_size(SCAN_MAP_MAX_SIZE, sizeof(ScanHashEntry)));
	size = add_size(size, hash_estimate_size(BLOCK_MAP_MAX_SIZE, sizeof(BlockGroupHashEntry)));
// ### total size estimate for the lists of block groups
//	size = add_size(size, sizeof(BlockGroupData) * ???)
// ### total size estimate of list of scans on each block group
//	size = add_size(size, sizeof(BlockGroupScanList) * ???)
// ### set of buffers in each block group doesn't take additional space, embedded in the buffer header

// ### ADD SIZE ESTIMATES


	// actually estimate the size later... for now assume 100 MiB will be enough (maybe increase this :)
	size = add_size(size, 100 << 6);
#endif // USE_PBM
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
	BlockGroupHashEntry * b_entry;
	const BlockNumber startblock = scan->rs_startblock;
	const BlockNumber nblocks = scan->rs_nblocks;
	const BlockNumber nblock_groups = BLOCK_GROUP(nblocks);
	int bgnum;
	const float init_est_speed = pbm->initial_est_speed;

	tbl = (TableData){
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
	};

	// Insert the scan metadata
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		// Generate scan ID
		id = pbm->next_id;
		pbm->next_id += 1;

		// Create s_entry for the scan in the hash map
		s_data = search_scan(id, HASH_ENTER, &found);
		Assert(!found); // should be inserted...

		*s_data = (ScanData) {
				.last_pos = 0,
				.startBlock = startblock,
				.nblocks = nblocks,
				.est_speed = init_est_speed,
				.blocks_scanned = 0,
				.last_report_time = clock(),
		};
	}

	// Register the scan with each buffer
	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {

		// Insert into the buffer map as well
		b_entry = hash_search(pbm->BufferMap, &tbl, HASH_ENTER, &found);
		if (false == found) {
			const uint32 len = nblock_groups;
			const uint32 cap = len + (len >> 2); // ### Max(5, (len >> 2)); // allocate with 25% extra capacity

			b_entry->val = (BlockGroupDataVec){
				.len = len,
				.capacity = cap,
				.blockGroups = ShmemAlloc(cap * sizeof(BlockGroupData)),
			};
			for (uint32 i = 0; i < cap; ++i) {
				b_entry->val.blockGroups[i] = (BlockGroupData){
					.scans_head = NULL,
					.buffers_head = FREENEXT_END_OF_LIST,
				};
			}
		} else {
			const uint32 new_len = nblock_groups;
			const uint32 old_len = b_entry->val.len;

			b_entry->val.len = Max(b_entry->val.len, new_len);

			// Entry already present but capacity isn't large enough, reallocate.
// ### NOTE: this is not really supported...
			if (new_len > b_entry->val.capacity) {
				BlockGroupData* old = b_entry->val.blockGroups;
				const uint32 old_cap = b_entry->val.capacity;
				const uint32 new_cap = Max(old_cap * 2, new_len + (new_len >> 2));
				uint32 i;

				// ### this is not really supported...
				elog(WARNING, "Increasing capacity of PBM buffer array for table %s", scan->rs_base.rs_rd->rd_rel->relname.data);

				b_entry->val = (BlockGroupDataVec){
					.len = new_len,
					.capacity = new_cap,
					.blockGroups = ShmemAlloc(new_cap * sizeof(BlockGroupData)),
				};

				for (i = 0; i < old_len; ++i) {
					b_entry->val.blockGroups[i] = old[i];
				}
				for (	  ; i < new_cap; ++i) {
					b_entry->val.blockGroups[i] = (BlockGroupData){
						.scans_head = NULL,
						.buffers_head = FREENEXT_END_OF_LIST,
					};
				}

// ### leak old! since we can't free it...
				// ShmemFree(old);
			}
		}

// ### vvv below could be done outside the lock if each buffer had its own lock for its list.

		// Add the scan for each block group
		// First, use as many freed buffer-stats structs as we can
		for (bgnum = 0; pbm->buffer_stats_free_list != NULL && bgnum < nblock_groups; ++bgnum) {
			// Pop list element off the free-list
			BlockGroupScanList *const temp = pbm->buffer_stats_free_list;
			pbm->buffer_stats_free_list = temp->next;

			// Initialize the list element
			InitScanStatsEntry(temp, id, s_data, b_entry->val.blockGroups[bgnum].scans_head, bgnum);

			// Link it in
			b_entry->val.blockGroups[bgnum].scans_head = temp;
		}

		// Once we run out: allocate more (as a single allocation - it won't get free'd anyway)
		if (bgnum < nblock_groups) {
			// remember how many have been assigned already since temp and buffers array need to be indexed at different locations.
			const uint32 offset = bgnum;
			BlockGroupScanList *const new_stats = ShmemAlloc((nblock_groups - bgnum) * sizeof(BlockGroupScanList));

			for ( ; bgnum < nblock_groups; ++bgnum) {
				BlockGroupScanList *const temp = &new_stats[bgnum - offset];
				InitScanStatsEntry(temp, id, s_data, b_entry->val.blockGroups[bgnum].scans_head, bgnum);
				b_entry->val.blockGroups[bgnum].scans_head = temp;
			}
		}

		// TODO calculate the PRIORITY of each block (group)!
		// (actually, what do we know other than the first one is needed immediately?)

	} // LOCK_GUARD

	// Scan remembers the ID
	scan->scanId = id;

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
	ScanData data;
	const TableData tbl = (TableData){
			.rnode = scan->rs_base.rs_rd->rd_node,
			.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
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
		data = *search_scan(id, HASH_FIND, &found);
		Assert(found);

		// delete
		search_scan(id, HASH_REMOVE, &found);

		// Update global initial speed estimate: geometrically-weighted average
		// ### Note: we don't really care about lost updates here, could put this outside the lock (make volatile)
		new_est = pbm->initial_est_speed * alpha + data.est_speed * (1 - alpha);
		pbm->initial_est_speed = new_est;
	}


	// For each block in the scan: remove it from the list of scans
	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
		BlockGroupHashEntry *const b_entry = hash_search(pbm->BufferMap, &tbl, HASH_FIND, &found);
		if (false == found || NULL == b_entry) {
			elog(WARNING, "UnregisterSeqScan(%lu): could not find table in BufferMap", id);
		} else {

// ### vvv this could be done outside the lock (with only shared access) if we have locks for each buffer.
			// Iterate over the buffers to remove the scan metadata
			const uint32 upper = Min(BLOCK_GROUP(data.nblocks), b_entry->val.len);
			for (int64 i = (int64)(upper) - 1; i >= 0; --i) {
				DeleteScanFromGroup(id, &b_entry->val.blockGroups[i]);
// ### If accessing the buffers can remove the scan metadata when done, we should stop once we don't find it.
			}
		}
	}
}


void ReportSeqScanPosition(ScanId id, BlockNumber pos) {
	bool found;
	clock_t curTime, elapsed;
	ScanData *entry;
	BlockNumber blocks;
	BlockNumber prevGroupPos, curGroupPos;
	float speed;
	TableData tbl;

// ### how often to update stats? (see what sync scan does)

	// Only update stats periodically
	if (pos != GROUP_TO_FIRST_BLOCK(BLOCK_GROUP(pos))) {
		// do nothing in these cases
		return;
	}

	// Find the scan stats
	LOCK_GUARD_V2(PbmScansLock, LW_SHARED) {
		entry = search_scan(id, HASH_FIND, &found);
	}
	Assert(found);
	if (false == found || NULL == entry) {
		elog(WARNING, "ReportSeqScanPosition(%lu): could not find scan in ScanMap", id);
		return;
	}
	prevGroupPos = BLOCK_GROUP(entry->last_pos);
	curGroupPos = BLOCK_GROUP(pos);

	// Note: the entry is only *written* in one process.
	// If readers aren't atomic: how bad is this? Could mis-predict next access time...
	curTime = clock();
	elapsed = curTime - entry->last_report_time;
	if (pos > entry->last_pos) {
		blocks = pos - entry->last_pos;
	} else {
		// looped around back to the start block
		blocks = pos + entry->nblocks - entry->last_pos;
	}
	speed = (float)(blocks) / (float)(elapsed);
// ### estimating speed: should do better than this. e.g. exponentially weighted average, or moving average.
// ### MAYBE some kind of locking? Spin-lock should be good enough but a read-write spin lock would be preferable...
	entry->last_report_time = curTime;
	entry->last_pos = pos;
	entry->blocks_scanned += blocks;
	entry->est_speed = speed;


	// Remove the scan from blocks in range [prevGroupPos, curGroupPos)
	if (curGroupPos != prevGroupPos) {
		tbl = (TableData){ .rnode = entry->rnode, .forkNum = entry->forkNum, };
		LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
			const BlockGroupHashEntry * const b_entry = hash_search(pbm->BufferMap, &tbl, HASH_FIND, &found);
			BlockNumber upper = curGroupPos;
			Assert(found && b_entry != NULL);

			// special handling if we wrapped around (note: if we update every block group this probably does nothing
			if (curGroupPos < prevGroupPos) {
				// First delete the start part
				for (BlockNumber i = 0; i < curGroupPos; ++i) {
					DeleteScanFromGroup(id, &b_entry->val.blockGroups[i]);
				}
				// find last block
				upper = BLOCK_GROUP(entry->nblocks);
			}

			// Remove the scan from the blocks
			for (BlockNumber i = prevGroupPos; i < upper; ++i) {
				DeleteScanFromGroup(id, &b_entry->val.blockGroups[i]);
			}
		}
	}
	

#if defined(TRACE_PBM) && defined(TRACE_PBM_REPORT_PROGRESS)
	elog(INFO, "ReportSeqScanPosition(%lu) at block %d (group=%d), elapsed=%ld, blocks=%d, est_speed=%f",
		 id,
		 pos,
		 BLOCK_GROUP(pos),
		 elapsed,
		 blocks,
		 speed
	 );
#endif


// TODO: maybe want to track whether scan is forwards or backwards...
}

// ### revisit whether to register buffers when loaded or when unpinned
// Notify the PBM about a new buffer so it can be added to the priority queue
void PbmNewBuffer(BufferDesc * const buf) {
	const BlockGroupData* group;
#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	if (FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_next) {
		debug_buffer_access(buf, "new buffer");
	} else {
		debug_buffer_access(buf, "(WARNING!) new buffer already in block group, unlink first...");
	}
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_NEW

	RemoveBufFromBlock(buf);

#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	elog(WARNING, "PbmNewBuffer added new buffer:" //"\n"
			   "\tnew={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u (%u) %d/%d}",
		 buf->buf_id,
		 buf->tag.rnode.spcNode,
		 buf->tag.rnode.dbNode,
		 buf->tag.rnode.relNode,
		 buf->tag.blockNum,
		 BLOCK_GROUP(buf->tag.blockNum),
		 buf->pbm_bgroup_next, buf->pbm_bgroup_prev
	);
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_NEW


#ifdef SANITY_PBM_BUFFERS
	group = AddBufToBlock(buf);

	// TODO stop here if there is no group
	if (group == NULL) return;

	Assert(group->buffers_head == buf->buf_id);

	sanity_check_verify_block_group_buffers(buf);

#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	BufferDesc* temp = GetBufferDescriptor(group->buffers_head);
	BufferDesc* temp2 = temp->pbm_bgroup_next < 0 ? NULL : GetBufferDescriptor(temp->pbm_bgroup_next);

	if (temp2 != NULL) {
		elog(INFO, "PbmNewBuffer added new buffer:"
				   "\n\t new={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}"
				   "\n\t old={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}",
			 temp->buf_id,
			 temp->tag.rnode.spcNode,
			 temp->tag.rnode.dbNode,
			 temp->tag.rnode.relNode,
			 temp->tag.blockNum,
			 BLOCK_GROUP(temp->tag.blockNum),
			 temp->pbm_bgroup_prev, temp->pbm_bgroup_next
			 ,
			 temp2->buf_id,
			 temp2->tag.rnode.spcNode,
			 temp2->tag.rnode.dbNode,
			 temp2->tag.rnode.relNode,
			 temp2->tag.blockNum,
			 BLOCK_GROUP(temp2->tag.blockNum),
			 temp2->pbm_bgroup_prev, temp2->pbm_bgroup_next
		);
	} else {
		elog(INFO, "PbmNewBuffer added new buffer:"
				   "\n\t new={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}"
			 	   "\n\t old={n/a}",
			 temp->buf_id,
			 temp->tag.rnode.spcNode,
			 temp->tag.rnode.dbNode,
			 temp->tag.rnode.relNode,
			 temp->tag.blockNum,
			 BLOCK_GROUP(temp->tag.blockNum),
			 temp->pbm_bgroup_next, temp->pbm_bgroup_prev
		);
	}
#endif // tracing
#endif // SANITY_PBM_BUFFERS

	// TODO if group wasn't in the PQ already, add it
}


#if 0
// Notify PBM about a buffer which could get added to the priority queue.
void PbmBufferNotPinned(struct BufferDesc* buf){
	BlockGroupData* group;
#ifdef TRACE_PBM
	if (FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_next) {
		debug_buffer_access(buf, "unpin buffer NOT there yet!");
	} else {
		debug_buffer_access(buf, "unpin buffer already in block group");
	}
#endif // TRACE_PBM

	group = AddBufToBlock(buf);

// ### What if buffer isn't for this block? (evicting needs to remove it obviously)
// ### recalculate priority of the group?
}
#endif


// ### eventually want to get rid of this
inline void PbmOnEvictBuffer(struct BufferDesc *const buf) {
#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_EVICT)
	static int num_evicted = 0;
	elog(WARNING, "evicting buffer %d tbl={spc=%u, db=%u, rel=%u, fork=%d} block#=%u, #evictions=%d",
		 buf->buf_id,
		 buf->tag.rnode.spcNode,
		 buf->tag.rnode.dbNode,
		 buf->tag.rnode.relNode,
		 buf->tag.forkNum,
		 buf->tag.blockNum,
		 num_evicted++
	);
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

	// Buffer is not in any list (special thing here...
	if (-3 == it->pbm_bgroup_prev) {
		// ### this should eventually be disallowed
		Assert(-3 == it->pbm_bgroup_next);
		return;
	}

	Assert(it->pbm_bgroup_prev != FREENEXT_NOT_IN_LIST && it->pbm_bgroup_next != FREENEXT_NOT_IN_LIST);

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

void list_all_buffers() {
	for (int i = 0; i < NBuffers; ++i) {
		BufferDesc *buf = GetBufferDescriptor(i);
		BufferTag tag = buf->tag;
		elog(WARNING, "BLOCK %d: \tspc=%u, db=%u, rel=%u, fork=%d, block=%u (group=%u) \tprev=%d  next=%d",
			 i, tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum,
			 BLOCK_GROUP(tag.blockNum), buf->pbm_bgroup_prev, buf->pbm_bgroup_next
		);

		bool found;
		BlockGroupData *data = search_block_group(buf, &found);

		if (!found || data == NULL) {
			elog(WARNING, "\tGROUP NOT FOUND!  prev=%d  next=%d", buf->pbm_bgroup_prev, buf->pbm_bgroup_next);
			continue;
		}

		Buffer bid;
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
	BlockNumber blockNum = buf->tag.blockNum;
	BlockNumber blockGroup = BLOCK_GROUP(blockNum);
	TableData tbl = (TableData){
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
	};
	clock_t next_access_time = NotRequested;
	clock_t now = clock();

	const BlockGroupData *const block_scans = search_block_group(buf, &found);
	if (true == found) {
		next_access_time = PageNextConsumption(block_scans, &requested);
	}

	char* msg2;
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

// Search scan map for given scan and return reference to the data only (not including key)
ScanData* search_scan(const ScanId id, HASHACTION action, bool* foundPtr) {
	return &((ScanHashEntry*)hash_search(pbm->ScanMap, &id, action, foundPtr))->data;
}

// Search block group map for a particular buffer returning the location in the array
BlockGroupData * search_block_group(const BufferDesc *const buf, bool* foundPtr) {
	const BlockNumber blockNum = buf->tag.blockNum;
	const BlockNumber bgroup = BLOCK_GROUP(blockNum);
	const TableData tbl = (TableData){
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
	};

	// Look up by the table
	const BlockGroupHashEntry *const b_entry = hash_search(pbm->BufferMap, &tbl, HASH_FIND, foundPtr);

	if (false == *foundPtr) return NULL;

	// Verify the block is in the array (within bounds)
	if (bgroup >= b_entry->val.len) {
		*foundPtr = false;
		return NULL;
	}

	return &b_entry->val.blockGroups[bgroup];
}

// Print debugging information for a single entry in the scans hash map.
void debug_append_scan_data(StringInfoData* str, ScanHashEntry* entry) {
	appendStringInfo(str, "{id=%lu, start=%u, nblocks=%u, last_post=%u, last_time=%ld, speed=%f}",
					 entry->tag,
					 entry->data.startBlock,
					 entry->data.nblocks,
					 entry->data.last_pos,
					 entry->data.last_report_time,
					 entry->data.est_speed
	);
}

// Print the whole scans map for debugging.
void debug_log_scan_map(void) {
	StringInfoData str;
	initStringInfo(&str);

	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
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

		hash_seq_init(&status, pbm->BufferMap);

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

// Estimate the next access time of a block group based on the scan progress
clock_t ScanTimeToNextConsumption(const BlockGroupScanList *const stats, bool* foundPtr) {
	const ScanId id = stats->scan_id;
	ScanData s_data;
	const BlockNumber blocks_behind = GROUP_TO_FIRST_BLOCK(stats->blocks_behind);
	BlockNumber blocks_scanned;
	BlockNumber blocks_remaining;

	LOCK_GUARD_V2(PbmScansLock, LW_SHARED) {
		s_data = *search_scan(id, HASH_FIND, foundPtr);
	}

	// Might be possible the scan has just ended
	if (false == *foundPtr) return NotRequested;

	// Estimate time to next access time
	// First: estimate distance (# blocks) to the block based on # of blocks scanned and position
	// 		of the block group in the scan
	// Then: distance/speed = time to next access (estimate)
	blocks_scanned = s_data.blocks_scanned;
	if (blocks_behind < blocks_scanned) {
		blocks_remaining = 0;
	} else {
		blocks_remaining = blocks_behind - blocks_scanned;
	}

	return (clock_t)((float)blocks_remaining / s_data.est_speed);
}

clock_t PageNextConsumption(const BlockGroupData *const stats, bool *requestedPtr) {
	clock_t min_next_access = -1;
	bool found = false;
	const BlockGroupScanList * it;

// ### lock the entry in some way?

	*requestedPtr = false;

	// not requested if there are no scans
	if (NULL == stats || NULL == stats->scans_head) {
		return NotRequested;
	}

	// loop over the scans and check the next access time estimate from that scan
	it = stats->scans_head;
	while (it != NULL) {
		const clock_t time_to_next_access = ScanTimeToNextConsumption(it, &found);
		if (true == found && (false == *requestedPtr || time_to_next_access < min_next_access)) {
			min_next_access = time_to_next_access;
			*requestedPtr = true;
		}
		it = it->next;
	}

	// return the soonest next access time if applicable
	if (false == *requestedPtr) {
		return NotRequested;
	} else {
		return clock() + min_next_access;
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
		it->next = pbm->buffer_stats_free_list;
		pbm->buffer_stats_free_list = it;
		return true;
	}
}

BlockGroupData * AddBufToBlock(BufferDesc *const buf) {
	bool found;
	BlockGroupData * group;
	Buffer group_head;

	Assert(buf->pbm_bgroup_next == FREENEXT_NOT_IN_LIST && buf->pbm_bgroup_prev == FREENEXT_NOT_IN_LIST);
	if (buf->pbm_bgroup_next != FREENEXT_NOT_IN_LIST) {
		Assert(buf->pbm_bgroup_prev != FREENEXT_NOT_IN_LIST);
		return NULL; // nothing to do
		// TODO should this be possible at all?
	}

	// Find the block group for this buffer
	group = search_block_group(buf, &found);
// ### lock the group for inserting the thing?

	if (false == found) {
		// TODO ... how to handle buffers not associated with any scans? (let normal buffer manager handle it I guess)
		// ### sanity check: use -3 as a signal
		buf->pbm_bgroup_prev = -3;
		buf->pbm_bgroup_next = -3;

		return NULL;
	}

	// Link the buffer into the block group chain of buffers
	group_head = group->buffers_head;
	group->buffers_head = buf->buf_id;
	buf->pbm_bgroup_next = group_head;
	buf->pbm_bgroup_prev = FREENEXT_END_OF_LIST;
	if (group_head != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(group_head)->pbm_bgroup_prev = buf->buf_id;
	}

	return group;
}


static inline
void RemoveBufFromBlock(BufferDesc *const buf) {
	int next, prev;
// ### locking needed? (lock first when calling?)

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
		Assert(found);
		group->buffers_head = next;
	}

	buf->pbm_bgroup_prev = FREENEXT_NOT_IN_LIST;
	buf->pbm_bgroup_next = FREENEXT_NOT_IN_LIST;
}

#if 0
void PagePush(/*TODO args --- page + header? */) {
	/*
	 * Recalculate priority of a block
	 *  1. Remove from bucket if applicable
	 *  2. Estimate next consumption time
	 *  3. Add to bucket
	 */
}

void RefreshRequestedBuckets(/*TODO args?*/) {
	/*
	 * 1. Shift buckets if needed
	 * 2. Create new buckets to fill gaps as needed
	 * 3. Re-calculate priority of pages in bucket -1 and free that bucket
	 */
}

void EvictPage(/*TODO argsg?*/) {
	/*
	 * 1. Evict the "not requested" bucket if not empty
	 * 2. Otherwise, pick the last non-empty bucket
	 * 3. Pick one page --- or multiple?
	 */
}
#endif

/*
 * TODO:
 *  - ...
 *  - revisit locking/concurrency control
 *  - revisit style for consistency: CamelCase vs snake_case?
 */
