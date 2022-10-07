#ifndef POSTGRESQL_PBM_INTERNAL_H
#define POSTGRESQL_PBM_INTERNAL_H

#include "lib/ilist.h"

#include "storage/pbm.h"
#include "storage/relfilenode.h"
#include "storage/spin.h"
#include "storage/lwlock.h"

///-------------------------------------------------------------------------
/// Constants
///-------------------------------------------------------------------------

#define NS_PER_MS 	(1000 * 1000)		// 10^6 ns per millisecond
#define NS_PER_SEC 	(1000 * NS_PER_MS) 	// 10^9 ns per second

// ### consider different "AccessTimeNotRequested", and also whether this should be signed or not
static const unsigned long AccessTimeNotRequested = (unsigned long)(-1l);
static const unsigned int PQ_BucketOutOfRange = (unsigned int)(-1);

///-------------------------------------------------------------------------
/// PBM Configuration
///-------------------------------------------------------------------------

/// How much to shift the block # by to find its group
#define BLOCK_GROUP_SHIFT 5 // 10

/// Size of the hash maps
static const long ScanMapMaxSize = 1024;
static const long BlockGroupMapMaxSize = (1 << 16);

/// Clock to use
#define PBM_CLOCK CLOCK_MONOTONIC // CLOCK_MONOTONIC_COARSE

/// PQ configuration
#define PQ_NUM_NULL_BUCKETS 128
static const int PQ_NumBucketGroups = 10;
static const int PQ_NumBucketsPerGroup = 5;
static const int PQ_NumBuckets = PQ_NumBucketGroups * PQ_NumBucketsPerGroup;
static const uint64_t PQ_TimeSlice = 100 * NS_PER_MS;

/*
 * Whether tp use spinlocks or LWLocks for PBM PQ buckets.
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


/// Debugging flags
//#define TRACE_PBM
//#define TRACE_PBM_REPORT_PROGRESS
//#define TRACE_PBM_PRINT_SCANMAP
//#define TRACE_PBM_PRINT_BLOCKMAP
//#define TRACE_PBM_BUFFERS
//#define TRACE_PBM_BUFFERS_NEW
//#define TRACE_PBM_BUFFERS_EVICT
//#define TRACE_PBM_PQ_REFRESH
//#define TRACE_PBM_EVICT

//#define SANITY_PBM_BUFFERS
//#define SANITY_PBM_BLOCK_GROUPS


///-------------------------------------------------------------------------
/// Helper macros & inline functions
///-------------------------------------------------------------------------

/// Helper to make sure locks get freed
/// NOTE: do NOT `return` or `break` from inside the lock guard (break inside nested loop is OK), the lock won't be released.
/// `continue` from the lock guard is OK, will go to the end.
/// `var` holds the return value of the LWLockAcquire call
#define LOCK_GUARD_WITH_RES(var, lock, mode) \
  for ( \
    bool (_c ## var) = true, pg_attribute_unused() (var) = LWLockAcquire((lock), (mode)); \
  	(_c ## var); \
    LWLockRelease((lock)), (_c ## var) = false)
#define LOCK_GUARD_V2(lock, mode) LOCK_GUARD_WITH_RES(_ignore_, lock, mode)

/// Group blocks by ID to reduce the amount of metadata required
#define BLOCK_GROUP(block) ((block) >> BLOCK_GROUP_SHIFT)
#define GROUP_TO_FIRST_BLOCK(group) ((group) << BLOCK_GROUP_SHIFT)

/// Block group hash table segment:
#define LOG_BLOCK_GROUP_SEG_SIZE 8
#define BLOCK_GROUP_SEG_SIZE (1 << LOG_BLOCK_GROUP_SEG_SIZE)
#define BLOCK_GROUP_SEGMENT(group) ((group) >> LOG_BLOCK_GROUP_SEG_SIZE)

/// Convert a timestamp in ns to the corresponding timeslice in the PQ
static inline unsigned long ns_to_timeslice(unsigned long t) {
	return t / PQ_TimeSlice;
}


///-------------------------------------------------------------------------
/// Forward declarations
///-------------------------------------------------------------------------

struct BlockGroupData;
struct HTAB;

///-------------------------------------------------------------------------
/// Type definitions
///-------------------------------------------------------------------------

// Table identifier. Stored in the scan map
typedef struct TableData {
	RelFileNode	rnode; // physical relation
	ForkNumber	forkNum; // fork in the relation
} TableData;

/// Structs for storing information about scans in the hash map

// This struct stores scan stats that are only updated in the single process
// (in the main header file, because it is embedded in the scan operator)
typedef struct PBM_LocalScanStats LocalScanStats;

// Scan stats that need to be read by other threads when estimating scan speed
typedef struct PBM_SharedScanStats {
	BlockNumber	blocks_scanned;
	float		est_speed;
} SharedScanStats;

// Hash map value: scan info & statistics
// Note: records *blocks* NOT block *groups*
typedef struct PBM_ScanData {
	// Scan info (does not change after being set)
	TableData 	tbl; // the table being scanned
	BlockNumber startBlock; // where the scan started
	BlockNumber	nblocks; // # of blocks (not block *groups*) in the table

// ### add range information (later when looking at BRIN indexes)

	// Statistics written by one thread, but read by all
// ### needs to be atomic? or volatile good enough?
	_Atomic(SharedScanStats) stats;
} ScanData;

// Entry (KVP) in the scans hash map
typedef struct PBM_ScanHashEntry {
	ScanId		id;		// Hash key
	ScanData	data;	// Value
} ScanHashEntry;


/// Structs for information about block groups

// Info about a scan for the block group. (linked list)
typedef struct BlockGroupScanList {
	// Info about the current scan
	ScanId			scan_id;
	ScanHashEntry *	scan_entry;
	BlockNumber		blocks_behind;

// TODO replace scan_id with ptr to hash entry...

	// Next item in the list
	slist_node slist;
} BlockGroupScanList;

// Record data for this block group.
typedef struct BlockGroupData {
	// Locks for the below data structures
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLock lock;
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	slock_t slock;
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	slock_t scan_lock;
	slock_t buf_lock;
#else
#error "unknown block group lock mode"
#endif // PBM_BG_LOCK_MODE

	// Set of scans which care about this block group
	slist_head scans_list;

	// Set of (### unpinned?) buffers holding data in this block group
	volatile int buffers_head;

	// Linked-list in PQ buckets
	struct PbmPQBucket *volatile pq_bucket;
	dlist_node blist;
} BlockGroupData;

// Key in Block Group Data Map
typedef struct BlockGroupHashKey {
	RelFileNode	rnode;		// physical relation
	ForkNumber	forkNum;	// fork in the relation
	uint32		seg;		// "block group segment" in the hash table
} BlockGroupHashKey;

// Entry in Block Group Data Map
typedef struct BlockGroupHashEntry {
	// hash key
	BlockGroupHashKey	key;

	// previous and next segment
	struct BlockGroupHashEntry * seg_next;
	struct BlockGroupHashEntry * seg_prev;

	// list of groups in the segment
	BlockGroupData		groups[BLOCK_GROUP_SEG_SIZE];
} BlockGroupHashEntry;


/// The priority queue structure data structures for tracking blocks to evict
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

// The actual PQ structure
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
// ### keep track of "not requested" and "very far future" separately?
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

	_Atomic(unsigned long) last_shifted_time_slice;
} PbmPQ;


/// Main PBM data structure
typedef struct PbmShared {
	/* These fields never change after initialization, so no locking needed */

	/// Time-related stuff
	time_t start_time_sec;

	/* These fields have their own lock here */

	/// Free list of BlockGroupScanList elements to be re-used
	slock_t scan_free_list_lock;
	slist_head bg_scan_free_list;

	/* These fields protected by PbmScansLock: */

	/// Map[ scan ID -> scan stats ] to record progress & estimate speed
	struct HTAB * ScanMap;
	/// Counter for ID generation
	ScanId next_id;
	/// Global estimate of speed used for *new* scans
	float initial_est_speed;
// ### could make this atomic and move outside the scans lock in UnregisterSeqScan.


	/* These fields protected by PbmBlocksLock: */

	/// Map[ (table, BlockGroupSegment) -> set of scans ] + free-list of the list-nodes for tracking statistics on each buffer
	struct HTAB * BlockGroupMap;


	/* Protected by PbmPqBucketsLock and PbmEvictionLock */

	/// Priority queue of block groups that could be evicted
	PbmPQ * BlockQueue;

} PbmShared;


///-------------------------------------------------------------------------
/// Global variables
///-------------------------------------------------------------------------

/// Global pointer to the single PBM
extern PbmShared * pbm;

///-------------------------------------------------------------------------
/// PBM PQ Initialization
///-------------------------------------------------------------------------
extern PbmPQ * InitPbmPQ(void);
extern Size PbmPqShmemSize(void);

///-------------------------------------------------------------------------
/// PBM PQ manipulation
///-------------------------------------------------------------------------
extern void PQ_RefreshBlockGroup(BlockGroupData * block_group, unsigned long t, bool requested);
extern void PQ_RemoveBlockGroup(BlockGroupData * block_group);
extern bool PQ_ShiftBucketsWithLock(unsigned long ts);
extern bool PQ_CheckEmpty(void);
#if PBM_EVICT_MODE == PBM_EVICT_MODE_SINGLE
extern struct BufferDesc * PQ_Evict(PbmPQ * pq, uint32 * but_state);
#endif

///-------------------------------------------------------------------------
/// Helpers
///-------------------------------------------------------------------------
extern unsigned long get_time_ns(void);
extern unsigned long get_timeslice(void);


/// Block group locking

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


#endif //POSTGRESQL_PBM_INTERNAL_H
