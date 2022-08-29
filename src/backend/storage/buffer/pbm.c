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
#include "access/heapam.h"

#include <time.h>


// TODO! look for ### comments --- low-priority/later TODOs


///-------------------------------------------------------------------------
/// Configuration
///-------------------------------------------------------------------------

// Output debugging messages
#define TRACE_PBM
//#define TRACE_PBM_REPORT_PROGRESS
#define TRACE_PBM_PRINT_SCANMAP
#define TRACE_PBM_PRINT_BLOCKMAP

// how much to shift the block # by to find its group
#define BLOCK_GROUP_SHIFT 5 // 10

const clock_t NotRequested = 0;

///-------------------------------------------------------------------------
/// Helper macros
///-------------------------------------------------------------------------

/// Helper to make sure locks get freed
/// NOTE: do NOT return from inside the lock guard (or break/continue if it will leave the scope), the lock won't be released.
#define LOCK_GUARD_V1(lock, mode, code) LWLockAcquire((lock), (mode)); {code} LWLockRelease((lock));
#define LOCK_GUARD_V2(lock, mode) \
  for (bool _c_ = true, pg_attribute_unused() _ = LWLockAcquire((lock), (mode)); _c_; LWLockRelease((lock)), _c_ = false)

/// Block size is too small to store *ever* block individually, shift the ID to reduce the $ of blocks.
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
// ### Consider storing these atomically in a single 8-byte value...
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


/// Structs for information about buffers
// Hash key: table identifier
typedef struct TableData {
	RelFileNode	rnode;
	ForkNumber	forkNum;
} TableData;

// Info about a scan for the buffer.
typedef struct BlockGroupScanStats {
	ScanId		scan_id;
	BlockNumber	blocks_behind;

// TODO remember which buffers are actually in the cache? (more than one...)

// ### make linked-list of short arrays to improve performance (less pointer chasing and less frequent allocations)

	// Store linked-list of scan stats
	struct BlockGroupScanStats* next;
} BlockGroupScanStats;

typedef struct BufferScanList {
// ### some kind of lock?
	BlockGroupScanStats* head;
} BufferScanList;

// Hash key: set of scans on the buffer
typedef struct BufferDataVec {
// ### maybe want a (spin?) lock here for searching
	uint32	len; // # of block *groups* in the table
	uint32	capacity;

	// Each is a *set* (array) of scan information
	BufferScanList*	buffers;
} BufferDataVec;

// Entry (KVP) in the hash map
typedef struct BufferHashEntry {
	TableData		key;
	BufferDataVec	val;
} BufferHashEntry;

/// Main PBM data structure
typedef struct PbmShared {
	// TODO more granular locking if it could improve performance.

	/// Atomic counter for ID generation
	/// Note: protected by PbmScansLock: could make it atomic but it is only accessed when we lock the hash table anyways.
	ScanId next_id;

	/// Map[ scan ID -> scan stats ] to record progress & estimate speed
	/// Protected by PbmScansLock
	HTAB * ScanMap;

// ### Map[ range (table -> block indexes) -> scan ] --- for looking up what scans reference a particular block, maybe useful later
	//	HTAB * ScansByRange;

	/// Map[ table -> buffer -> set of scans ] + free-list of the list-nodes for tracking statistics on each buffer
	/// Protected by PbmBlocksLock
	HTAB * BufferMap;
	BlockGroupScanStats* buffer_stats_free_list;
// ### if possible, free list could be protected by its own spinlock instead of using this lock
// ### eventually: modify this to be more shared-memory friendly? B-Tree of blocks for each table?

	/// Global estimate of speed used for *new* scans
	/// Currently NOT protected, as long as write is atomic we don't really care about lost updates...
	float initial_est_speed;


// Potential other fields:
// Array[NBlocks] of SET of (scan id, tuples behind) (or map?)
// Probably need Map[ table -> cached buffers ] as well (with buffers sorted in some way for range scans)
} PbmShared;

/// Global pointer to the single PBM
PbmShared* pbm;


///-------------------------------------------------------------------------
/// Prototypes for private methods
///-------------------------------------------------------------------------

ScanData* search_scan(ScanId id, HASHACTION action, bool* found);

void InitScanStatsEntry(BlockGroupScanStats* temp, ScanId id, ScanData* sdata, BlockGroupScanStats* next, BlockNumber bgnum);

// debugging
void debug_append_scan_data(StringInfoData* str, ScanHashEntry* entry);
void debug_log_scan_map(void);
void debug_append_buffer_data(StringInfoData * str, BufferHashEntry * entry);
void debug_log_buffers_map(void);

// managing buffer priority
clock_t ScanTimeToNextConsumption(BlockGroupScanStats* stats, bool* found);
clock_t PageNextConsumption(BufferScanList *stats, bool *requested);


///-------------------------------------------------------------------------
/// Initialization methods
///-------------------------------------------------------------------------

void InitPBM(void) {
	bool found;
	int hash_flags;
	HASHCTL hash_info;

	// Initialize the PBM
	pbm = (PbmShared*) ShmemInitStruct("Predictive buffer manager", sizeof(PbmShared), &found);

	// If the PBM was already initialized, nothing to do.
	if (found) {
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
		.entrysize = sizeof(BufferHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->BufferMap = ShmemInitHash("PBM buffer stats", 32, 1024, &hash_info, hash_flags);



// ### other fields to be added...
}


Size PbmShmemSize(void) {
	Size size = 0;

	size = add_size(size, sizeof(PbmShared));

// ### ADD SIZE ESTIMATES


	// actually estimate the time later... for now assume 100 MiB will be enough (maybe increase this :)
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
	BufferHashEntry * b_entry;
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
		if (!found) {
			uint32 len = nblock_groups;
			uint32 cap = len + (len >> 2); // ### Max(5, (len >> 2)); // allocate with 25% extra capacity

			b_entry->val = (BufferDataVec){
				.len = len,
				.capacity = cap,
				.buffers = ShmemAlloc(cap * sizeof(BlockGroupScanStats*)),
			};
			for (uint32 i = 0; i < cap; ++i) {
				b_entry->val.buffers[i] = (BufferScanList){
					.head = NULL,
				};
			}
		} else {
			uint32 new_len = nblock_groups;
			uint32 old_len = b_entry->val.len;

			b_entry->val.len = Max(b_entry->val.len, new_len);

			// Entry already present but capacity isn't large enough, reallocate.
// ### NOTE: this is not really supported...
			if (new_len > b_entry->val.capacity) {
				BufferScanList* old = b_entry->val.buffers;
				uint32 old_cap = b_entry->val.capacity;
				uint32 new_cap = Max(old_cap * 2, new_len + (new_len >> 2));
				uint32 i;

				// ### this is not really supported...
				elog(WARNING, "Increasing capacity of PBM buffer array for table %s", scan->rs_base.rs_rd->rd_rel->relname.data);

				b_entry->val = (BufferDataVec){
					.len = new_len,
					.capacity = new_cap,
					.buffers = ShmemAlloc(new_cap * sizeof(BufferScanList)),
				};

				for (i = 0; i < old_len; ++i) {
					b_entry->val.buffers[i] = old[i];
				}
				for (	  ; i < new_cap; ++i) {
					b_entry->val.buffers[i] = (BufferScanList){
						.head = NULL,
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
			BlockGroupScanStats* temp = pbm->buffer_stats_free_list;
			pbm->buffer_stats_free_list = pbm->buffer_stats_free_list->next;

			// Initialize the list element
			InitScanStatsEntry(temp, id, s_data, b_entry->val.buffers[bgnum].head, bgnum);

			// Link it in
			b_entry->val.buffers[bgnum].head = temp;
		}

		// Once we run out: allocate more (as a single allocation - it won't get free'd anyway)
		if (bgnum < nblock_groups) {
			// remember how many have been assigned already since temp and buffers array need to be indexed at different locations.
			const uint32 offset = bgnum;
			BlockGroupScanStats* new_stats = ShmemAlloc((nblock_groups - bgnum) * sizeof(BlockGroupScanStats));

			for ( ; bgnum < nblock_groups; ++bgnum) {
				BlockGroupScanStats* temp = &new_stats[bgnum - offset];
				InitScanStatsEntry(temp, id, s_data, b_entry->val.buffers[bgnum].head, bgnum);
				b_entry->val.buffers[bgnum].head = temp;
			}
		}

		// TODO calculate the PRIORITY of each block (group)!
		// (actually, what do we know other than the first one is needed immediately?)

	} // LOCK_GUARD


	// NOTE: we CANNOT store the HeapScanDesc directly in the hash map, since it is shared! ... unless we only care about it locally?
	// ### can we have separate (local) maps for tracking scans??


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
	ScanId id = scan->scanId;
	bool found;
	ScanData data;
	TableData tbl = (TableData){
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
		const float alpha = 0.85f;
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
		BufferHashEntry * b_entry = hash_search(pbm->BufferMap, &tbl, HASH_FIND, &found);
		if (!found || b_entry == NULL) {
			elog(WARNING, "UnregisterSeqScan(%lu): could not find table in BufferMap", id);
		} else {

// ### vvv this could be done outside the lock (with only shared access) if we have locks for each buffer.
			// Iterate over the buffers to remove the scan metadata
			const uint32 upper = Min(BLOCK_GROUP(data.nblocks), b_entry->val.len);
			for (int64 i = (int64)(upper) - 1; i >= 0; --i) {
				BlockGroupScanStats *it = b_entry->val.buffers[i].head;
				BlockGroupScanStats **prev = &b_entry->val.buffers[i].head;

				// find the scan in the list
				while (it != NULL && it->scan_id != id) {
					prev = &it->next;
					it = it->next;
				}

				// if found: remove it
				if (it != NULL) {
					// unlink
					*prev = it->next;
					// add to free list
					it->next = pbm->buffer_stats_free_list;
					pbm->buffer_stats_free_list = it;
				}
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
	float speed;

// TODO don't update stats every time? only on certain block groups.

	LOCK_GUARD_V2(PbmScansLock, LW_SHARED) {
		entry = search_scan(id, HASH_FIND, &found);
	}
	Assert(found);
	if (!found || entry == NULL) {
		elog(WARNING, "ReportSeqScanPosition(%lu): could not find scan in ScanMap", id);
		return;
	}

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


///-------------------------------------------------------------------------
/// Private helpers:
///-------------------------------------------------------------------------

// Search scan map for given scan and return reference to the data only (not including key)
ScanData* search_scan(const ScanId id, HASHACTION action, bool* found) {
	return &((ScanHashEntry*)hash_search(pbm->ScanMap, &id, action, found))->data;
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
			if (entry == NULL) break;

			appendStringInfoString(&str, "\n\t");
			debug_append_scan_data(&str, entry);
		}
	}

	ereport(INFO,
			(errmsg_internal("PBM scan map:"),
					errdetail_internal("%s", str.data)));
	pfree(str.data);
}

// Print an entry in the buffers hash table for debugging. Note: the entry is for a *table* and includes an array of all buffers, so skip most of them.
void debug_append_buffer_data(StringInfoData * str, BufferHashEntry * entry) {
	const int skip_amt = 1000;

	appendStringInfo(str, "rel=%d [", entry->key.rnode.relNode);
	for (int i = 0; i < entry->val.len; i += skip_amt) {
		BlockGroupScanStats * stat = entry->val.buffers[i].head;

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
		BufferHashEntry * entry;

		hash_seq_init(&status, pbm->BufferMap);

		for(;;) {
			entry = hash_seq_search(&status);
			if (entry == NULL) break;

			appendStringInfoString(&str, "\n\t");
			debug_append_buffer_data(&str, entry);
		}
	}

	ereport(INFO,
			(errmsg_internal("PBM buffers map:"),
					errdetail_internal("%s", str.data)));
	pfree(str.data);
}


clock_t ScanTimeToNextConsumption(BlockGroupScanStats* stats, bool* found) {
	const ScanId id = stats->scan_id;
	ScanData s_data;
	const BlockNumber blocks_behind = GROUP_TO_FIRST_BLOCK(stats->blocks_behind);
	BlockNumber blocks_scanned;
	BlockNumber blocks_remaining;

	LOCK_GUARD_V2(PbmScansLock, LW_SHARED) {
		s_data = *search_scan(id, HASH_FIND, found);
	}

	// Might be possible the scan has just ended
	if (!found) return NotRequested;

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

clock_t PageNextConsumption(BufferScanList *stats, bool *requested) {
	clock_t min_next_access = -1;
	bool found = false;
	BlockGroupScanStats * it;

// ### lock the entry in some way?

	*requested = false;

	// not requested if there are no scans
	if (stats == NULL || stats->head == NULL) {
		return NotRequested;
	}

	// loop over the scans and check the next access time estimate from that scan
	it = stats->head;
	while (it != NULL) {
		const clock_t time_to_next_access = ScanTimeToNextConsumption(it, &found);
		if (found && (!*requested || time_to_next_access < min_next_access)) {
			min_next_access = time_to_next_access;
			*requested = true;
		}
		it = it->next;
	}

	// return the soonest next access time if applicable
	if (!*requested) {
		return NotRequested;
	} else {
		return clock() + min_next_access;
	}
}

// Initialize an entry in the list of scans for a block group
void InitScanStatsEntry(BlockGroupScanStats* temp, ScanId id, ScanData* sdata, BlockGroupScanStats* next, BlockNumber bgnum) {
	BlockNumber startblock = sdata->startBlock;
	BlockNumber nblocks = sdata-> nblocks;
	BlockNumber blocks_behind;
	// convert group # -> block #
	BlockNumber block_num = GROUP_TO_FIRST_BLOCK(bgnum);

	// calculate where the block group is in the scan relative to start block
	if (block_num >= startblock) {
		blocks_behind = block_num - startblock;
	} else {
		blocks_behind = block_num + nblocks - startblock;
	}

	// fill in data of the new list element
	*temp = (BlockGroupScanStats){
			.scan_id = id,
			.blocks_behind = blocks_behind,
			.next = next,
	};
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
 * TODO ALSO NEED:
 *  - un-pin page needs to know if we're done with that block, move to not-requested as appropriate
 *  - needs to re-calculate priority regardless
 */
