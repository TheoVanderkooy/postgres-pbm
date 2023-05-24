/*-------------------------------------------------------------------------
 * Internals of the Predictive Buffer Manager
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRESQL_PBM_INTERNAL_H
#define POSTGRESQL_PBM_INTERNAL_H

#include "lib/ilist.h"

#include "storage/pbm.h"
#include "storage/relfilenode.h"
#include "storage/spin.h"
#include "storage/lwlock.h"
#include "storage/off.h"


/*-------------------------------------------------------------------------
 * Constants
 *-------------------------------------------------------------------------
 */

#define NS_PER_US	1000				// 10^3 ns per microsecond
#define NS_PER_MS 	(1000 * NS_PER_US)	// 10^6 ns per millisecond
#define NS_PER_SEC 	(1000 * NS_PER_MS) 	// 10^9 ns per second

static const unsigned long AccessTimeNotRequested = (unsigned long)(-1l);
#if PBM_USE_PQ
static const unsigned int PQ_BucketOutOfRange = (unsigned int)(-1);
#endif

/*-------------------------------------------------------------------------
 * PBM Configuration
 *-------------------------------------------------------------------------
 */

/* Log of the number of block groups in 1 segment in the hash table. */
#define LOG_BLOCK_GROUP_SEG_SIZE 8

/* Hash map sizes */
static const long ScanMapMaxSize = 1024;
static const long BlockGroupMapMaxSize = (1 << 12);
static const long IndexScanMapMaxSize = 1024;
static const long IndexScanNumConcurrent = 1024;
/*
 * For 128 GiB = 2^37 bytes database:
 * Block size = 8192 = 2^13 bytes
 * Block group size = 2^5 blocks (for now)
 * Group segment size = 2^8 groups (for now)
 * ==> need 2^11 segments, over-estimate 2^12
 */

/*
 * What clock to use
 *
 * CLOCK_MONOTONIC_COARSE seems to have 4ms resolution, which *might* be enough
 * but unless it becomes a bottleneck the normal clock is probably good enough.
 * (though resolution ta least ~100us would be nice as a middle-ground)
 */
#define PBM_CLOCK CLOCK_MONOTONIC

#if PBM_USE_PQ
/* PQ configuration */
#define PQ_NUM_NULL_BUCKETS 128
static const int PQ_NumBucketGroups = 10;
static const int PQ_NumBucketsPerGroup = 5;
static const int PQ_NumBuckets = PQ_NumBucketGroups * PQ_NumBucketsPerGroup;
static const uint64_t PQ_TimeSlice = 100 * NS_PER_MS;
#endif /* PBM_USE_PQ */

/*
 * Whether to use spinlocks or LWLocks for PBM PQ buckets.
 * Probably spinlocks are better, but with both implementations we can test it!
 */
#define PBM_PQ_BUCKETS_USE_SPINLOCK


/*
 * What method of locking should be used for block groups.
 * 0: a single LWLock for each block group
 * 1: a single spinlock for each block group
 * 2: a separate spin lock for the scans list and buffers list
 * consider: a separate fixed set of locks instead of one per group
 * 	- less memory, and there are only so many locks we might by trying to use at once anyways!
 * 	- however gives a chance of "collision" when multiple groups are trying to be accessed with the same lock
 */
#define PBM_BG_LOCK_MODE_LWLOCK 0
#define PBM_BG_LOCK_MODE_SINGLE_SPIN 1
#define PBM_BG_LOCK_MODE_DOUBLE_SPIN 2
//#define PBM_BG_LOCK_MODE PBM_BG_LOCK_MODE_LWLOCK
//#define PBM_BG_LOCK_MODE PBM_BG_LOCK_MODE_SINGLE_SPIN
#define PBM_BG_LOCK_MODE PBM_BG_LOCK_MODE_DOUBLE_SPIN


/* Number of buckets for index scan stats. */
#define PBM_INDEX_SCAN_NUM_COUNTS (1 << 14)

/* Whether to use the inverse frequency counts. */
#define PBM_INDEX_SCAN_USE_COUNTS


/* Debugging flags */
//#define TRACE_PBM
//#define TRACE_PBM_REGISTER
//#define TRACE_PBM_REGISTER_INDEX
//#define TRACE_PBM_REPORT_PROGRESS
//#define TRACE_PBM_REPORT_PROGRESS_VERBOSE
//#define TRACE_PBM_BITMAP_PROGRESS
//#define TRACE_PBM_INDEX_PROGRESS
//#define TRACE_PBM_PRINT_SCANMAP
//#define TRACE_PBM_PRINT_BLOCKGROUPMAP
//#define TRACE_PBM_PRINT_IDXSCANMAP
//#define TRACE_PBM_BUFFERS
//#define TRACE_PBM_BUFFERS_NEW
//#define TRACE_PBM_BUFFERS_EVICT
//#define TRACE_PBM_IDX_EVICT_CALC
//#define TRACE_PBM_PQ_REFRESH
//#define TRACE_PBM_ON_BUFFER_ACCESS

//#define SANITY_PBM_BUFFERS /* Note: this one is only valid with no concurrency! */
//#define SANITY_PBM_BLOCK_GROUPS
//#define SANITY_PBM_SCAN_FULLY_UNREGISTERED


/*-------------------------------------------------------------------------
 * Helper macros
 *-------------------------------------------------------------------------
 */

/* Helper to make sure locks get freed
 * NOTE: do NOT `return` or `break` from inside the lock guard (break inside nested loop is OK), the lock won't be released.
 * `continue` from the lock guard is OK, will go to the end.
 * `var` holds the return value of the LWLockAcquire call
 */
#define LOCK_GUARD_WITH_RES(var, lock, mode) \
  for ( \
    bool (_c ## var) = true, pg_attribute_unused() (var) = LWLockAcquire((lock), (mode)); \
  	(_c ## var); \
    LWLockRelease((lock)), (_c ## var) = false)
#define LOCK_GUARD_V2(lock, mode) LOCK_GUARD_WITH_RES(_ignore_, lock, mode)

/* Group blocks by ID to reduce the amount of metadata required */
#define BLOCK_GROUP(block) ((block) >> PBM_BLOCK_GROUP_SHIFT)
#define GROUP_TO_FIRST_BLOCK(group) ((group) << PBM_BLOCK_GROUP_SHIFT)

/* Block group hash table segment: */
#define BLOCK_GROUP_SEG_SIZE (1 << LOG_BLOCK_GROUP_SEG_SIZE)
#define BLOCK_GROUP_SEGMENT(group) ((group) >> LOG_BLOCK_GROUP_SEG_SIZE)


/*-------------------------------------------------------------------------
 * Forward declarations
 *-------------------------------------------------------------------------
 */

struct BlockGroupData;
struct HTAB;


/*-------------------------------------------------------------------------
 * Type definitions
 *-------------------------------------------------------------------------
 */

/* Table identifier. Stored in the scan map */
typedef struct TableData {
	RelFileNode	rnode;		// physical relation
	ForkNumber	forkNum;	// fork in the relation
} TableData;


/*
 * Structs for storing information about scans in the hash map.
 */

/* Store sequential scan stats that are only updated in the single process */
typedef struct PBM_LocalSeqScanStats LocalSeqScanStats;

/* Sequential scan stats that need to be read by other threads when estimating scan speed */
typedef struct PBM_SharedScanStats {
	BlockNumber	blocks_scanned;
	float		est_speed;
} SharedScanStats;

/* Hash map value: scan info & statistics
 * Note: records *blocks* NOT block *groups*
 */
typedef struct PBM_ScanData {
	// Scan info (does not change after being set)
	TableData 	tbl;		// the table being scanned
	BlockNumber startBlock;	// where the scan started
	BlockNumber	nblocks;	// # of blocks (not block *groups*) in the table
	struct ParallelBlockTableScanDescData * pbscan;	// data for parallel sequential scans (null if not a parallel seq scan)

	// Statistics written by one thread, but read by all
	volatile _Atomic(SharedScanStats) stats;

	union {
		// Only used in "leader" of parallel sequential scan
		struct {
			uint32	nworkers;
			uint64	nalloced;
			BlockNumber	chunk_size;
		} pseq;
		// no other special cases for now
	};

} ScanData;

/* Entry (KVP) in the scans hash map */
typedef struct PBM_ScanHashEntry {
	ScanId		id;		// Hash key
	ScanData	data;	// Value
} ScanHashEntry;


/*
 * Structs for information about block groups
 */

/* Info about a scan for the block group. (linked list) */
typedef struct BlockGroupScanListElem {
	// Info about the current scan
	ScanId			scan_id;
	ScanHashEntry *	scan_entry;
	BlockNumber		blocks_behind;

// ### remove scan_id once it is clear we don't need it...

	// Next item in the list
	slist_node		slist;
} BlockGroupScanListElem;

/* Record data for this block group. */
typedef struct BlockGroupData {
	// Locks for the below data structures
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLock	lock;
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	slock_t	slock;
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	slock_t	scan_lock;
#if PBM_TRACK_BLOCKGROUP_BUFFERS
	slock_t	buf_lock;
#endif /* PBM_TRACK_BLOCKGROUP_BUFFERS */
#else
#error "unknown block group lock mode"
#endif // PBM_BG_LOCK_MODE

	// Set of scans which care about this block group
	slist_head scans_list;

#if PBM_TRACK_BLOCKGROUP_BUFFERS
	// Set of buffers holding data in this block group
	volatile int buffers_head;
#endif

#if PBM_USE_PQ
	// Linked-list in PQ buckets
	struct PbmPQBucket *volatile pq_bucket;
	dlist_node blist;
#endif /* PBM_USE_PQ */

	// cache of estimated next access time
	volatile _Atomic(unsigned long) est_next_access;
	volatile _Atomic(unsigned long) est_invalid_at;
} BlockGroupData;

/* BlockGroupHashKey defined in pbm.h */

/* Entry in Block Group Data Map */
typedef struct BlockGroupHashEntry {
	// Hash key
	BlockGroupHashKey	key;

	// Previous and next segment
	struct BlockGroupHashEntry * seg_next;
	struct BlockGroupHashEntry * seg_prev;

	// List of groups in the segment
	BlockGroupData		groups[BLOCK_GROUP_SEG_SIZE];
} BlockGroupHashEntry;

/* Stats for a single index scan */
typedef struct IndexScanStatsEntry {
	ScanId id;

	/* Planner stats */
	double plan_rows;
	double plan_loops;
	BlockNumber nblocks;
	double accesses_per_block;

	/* List entry + the hash entry it is in. Protected by the entry's lock */
	dlist_node dlist;
	struct IndexScanHashEntry * hash_entry;

	/* Runtime stats */
#if defined(TRACE_PBM_INDEX_PROGRESS)
	_Atomic(int) dbg_times_reported;
#endif /* TRACE_PBM_INDEX_PROGRESS */
	_Atomic(uint64) total_count;
	_Atomic(unsigned int) loop_count;

	/* These stats updated atomically */
	slock_t stats_lock;
	_Atomic(double) est_speed;
	unsigned long last_stats_update_t;
	uint64 last_stats_update_count;

	/* Delay of trailing scan if applicable */
	_Atomic(uint64) trailing_delay;

	/* If *this scan* is trailing something else: record info to "unregister" later */
	slock_t leading_scan_lock; // ### is this useful? (maybe for multiple workers in the same scan entry?
	struct IndexScanStatsEntry * leading_scan;
	ScanId leading_scan_id;
	uint64 leading_scan_delay;

#ifdef PBM_INDEX_SCAN_USE_COUNTS
	/* The array of access counts per block */
	_Atomic(uint16) counts[PBM_INDEX_SCAN_NUM_COUNTS];
#endif
} IndexScanStatsEntry;

/* Entry in Index scan map */
typedef struct IndexScanHashEntry {
	RelFileNode key;

	LWLock lock;
	dlist_head dlist;
} IndexScanHashEntry;

/*
 * The priority queue structure data structures for tracking blocks to evict
 */

#if PBM_USE_PQ
/* PQ bucket */
typedef struct PbmPQBucket {
	// Lock for this bucket
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	slock_t slock;
#else
	LWLock lock;
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK

	// List of BlockGroupData in the list
	dlist_head bucket_dlist;
} PbmPQBucket;

/* The actual PQ structure */
typedef struct PbmPQ {
	/*
	 * LOCKING:
	 *
	 * PbmPqBucketsLock: protects the list of buckets. Exclusive lock for shifting
	 * buckets, shared lock for everything else.
	 *
	 * PbmEvictionLock: lock exclusively when evicting. If we had to wait to
	 * acquire the lock, it means someone else was evicting something. So check
	 * the free list first before evicting again. Note: we still actually wait
	 * to acquire the lock (instead of doing a ConditionalAcquire) to avoid
	 * contention on the free-list spinlock.
	 */

	// List of buckets, protected by PbmPqBucketsLock
	PbmPQBucket ** buckets;

// ### can maybe get rid of "not_requested_other", only used for single-eviction mode (which I'm not sure it even works well)
	PbmPQBucket * not_requested_bucket;
	PbmPQBucket * not_requested_other;
	PbmPQBucket nr1;
	PbmPQBucket nr2;

	// Locks for "null" buckets
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	slock_t null_bucket_locks[PQ_NUM_NULL_BUCKETS];
#else
	LWLock null_bucket_locks[PQ_NUM_NULL_BUCKETS];
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK

	volatile _Atomic(unsigned long) last_shifted_time_slice;
} PbmPQ;
#endif

struct PbmBufferIdxScanMarker {
	uint64 access_time;
	struct IndexScanStatsEntry * entry;
	unsigned int scan_id;  /* to make sure we know the scan finishes and the struct is re-allocated to a new scan */
	unsigned int scan_loop_num;  /* to detect "rescan" */

	/* index relation */
	Oid idx_rel_id;
	/* tuple ID in the block */
	OffsetNumber tuple;
};

/*
 * Separate descriptor for PBM-related buffer fields.
 * (mainly here because I don't want to make the actual buffer descriptor >64B)
 */
typedef struct PbmBufferDescStats {
#if PBM_EVICT_MODE == PBM_EVICT_MODE_SAMPLING
	/* Block group in the PBM if applicable (protected by buffer header lock) */
	struct BlockGroupData * pbm_bg;
	/* Index scan stats for the buffer */
	struct IndexScanHashEntry * pbm_iscan_stats;
	/* List of index markers */
	slock_t idx_slock;
	struct PbmBufferIdxScanMarker idx_markers[PBM_IDX_NUM_MARKERS];
	_Atomic(uint64) trailing_idx_next_access;
#endif
#if PBM_TRACK_STATS
	slock_t slock;		/* lock for these fields */

#if PBM_BUFFER_STATS_MODE == PBM_BUFFER_STATS_MODE_NRECENT
	uint64 recent_accesses[PBM_BUFFER_NUM_RECENT_ACCESS];
#elif PBM_BUFFER_STATS_MODE == PBM_BUFFER_STATS_MODE_GEOMETRIC
	double avg_recent_inter_access;
	uint64 last_access;
	int nrecent_accesses;
#endif /* PBM_BUFFER_STATS_MODE */
#endif /* PBM_TRACK_STATS */
} PbmBufferDescStats;

typedef union PbmBufferDescStatsPadded {
	PbmBufferDescStats stats;
	char padding[192]; /* 64*4 */
} PbmBufferDescStatsPadded;

/*
 * Main PBM data structure
 */
typedef struct PbmShared {
	/* These fields never change after initialization, so no locking needed */

	/// Record initialization time as offset
	time_t start_time_sec;

	/// Extra buffer header fields...
	PbmBufferDescStatsPadded * buffer_stats;

	/* These fields have their own lock here */

	/// Free list of BlockGroupScanListElem elements to be re-used
	slock_t		scan_free_list_lock;
	slist_head	bg_scan_free_list;

	/* These fields protected by PbmScansLock: */

	/// Map[ scan ID -> scan stats ] to record progress & estimate speed
	struct HTAB * ScanMap;
	/// Counter for ID generation
	_Atomic(ScanId) next_id;
	/// Global estimate of speed used for *new* scans
	float initial_est_speed;


	/* These fields protected by PbmBlocksLock: */

	/// Map[ (table, BlockGroupSegment) -> set of scans ]
	struct HTAB * BlockGroupMap;


	/* These fields protected by PbmIdxScansLock: */
	/// Map[ table -> set of index scans ]
	struct HTAB * IndexScanMap;
	slock_t free_idxscan_stats_lock;
	dlist_head free_idxscan_stats;
#ifdef TRACE_PBM_REGISTER_INDEX
	_Atomic(int) dbg_alloced_index_scans;
#endif
	/// Global estimate of speed for *new* index scans
	_Atomic(float) initial_est_idx_speed;


	/* Debugging: track time required for eviction */
#if defined(PBM_TRACK_EVICTION_TIME)
	_Atomic(uint64) n_evictions;
	_Atomic(uint64) total_eviction_time;
#endif /* PBM_TRACK_EVICTION_TIME */
#if PBM_USE_PQ
	/* Protected by PbmPqBucketsLock and PbmEvictionLock */

	/// Priority queue of block groups that could be evicted
	PbmPQ * BlockQueue;
#endif /* PBM_USE_PQ */
} PbmShared;


/*-------------------------------------------------------------------------
 * Global variables
 *-------------------------------------------------------------------------
 */

/* Global pointer to the single PBM */
extern PbmShared * pbm;


#if PBM_USE_PQ
/*-------------------------------------------------------------------------
 * PBM PQ Initialization
 *-------------------------------------------------------------------------
 */

extern PbmPQ * InitPbmPQ(void);
extern Size PbmPqShmemSize(void);


/*-------------------------------------------------------------------------
 * PBM PQ manipulation
 *-------------------------------------------------------------------------
 */

extern void PQ_RefreshBlockGroup(BlockGroupData * block_group, unsigned long t, bool requested);
extern void PQ_RemoveBlockGroup(BlockGroupData * block_group);
extern bool PQ_ShiftBucketsWithLock(unsigned long ts);
extern bool PQ_CheckEmpty(void);
#if PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_SINGLE
extern struct BufferDesc * PQ_Evict(PbmPQ * pq, uint32 * but_state);
#endif
#endif /* PBM_USE_PQ */


/*-------------------------------------------------------------------------
 * Helpers
 *-------------------------------------------------------------------------
 */

#if PBM_USE_PQ
/* Convert a timestamp in ns to the corresponding timeslice in the PQ */
static inline unsigned long ns_to_timeslice(unsigned long t) {
	return t / PQ_TimeSlice;
}
#endif /* PBM_USE_PQ */


/*
 * Block group locking
 */

static inline void bg_lock_scans(BlockGroupData * bg, pg_attribute_unused() LWLockMode mode) {
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLockAcquire(&bg->lock, mode);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	SpinLockAcquire(&bg->slock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	SpinLockAcquire(&bg->scan_lock);
#endif // PBM_BG_LOCK_MODE
}

static inline void bg_unlock_scans(BlockGroupData * bg) {
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLockRelease(&bg->lock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	SpinLockRelease(&bg->slock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	SpinLockRelease(&bg->scan_lock);
#endif // PBM_BG_LOCK_MODE
}

#if PBM_TRACK_BLOCKGROUP_BUFFERS
static inline void bg_lock_buffers(BlockGroupData * bg, pg_attribute_unused() LWLockMode mode) {
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLockAcquire(&bg->lock, mode);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	SpinLockAcquire(&bg->slock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	SpinLockAcquire(&bg->buf_lock);
#endif // PBM_BG_LOCK_MODE
}

static inline void bg_unlock_buffers(BlockGroupData * bg) {
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLockRelease(&bg->lock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	SpinLockRelease(&bg->slock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	SpinLockRelease(&bg->buf_lock);
#endif // PBM_BG_LOCK_MODE
}
#endif /* PBM_TRACK_BLOCKGROUP_BUFFERS */




// debugging
// ### clean this up eventually
void debug_print_idxscan_data(void);
void debug_log_scan_map(void);
void debug_log_blockgroup_map(void);
#if PBM_TRACK_BLOCKGROUP_BUFFERS
void debug_log_find_blockgroup_buffers(void);
#endif /*PBM_TRACK_BLOCKGROUP_BUFFERS*/

#endif //POSTGRESQL_PBM_INTERNAL_H
