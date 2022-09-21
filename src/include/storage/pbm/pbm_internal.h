#ifndef POSTGRESQL_PBM_INTERNAL_H
#define POSTGRESQL_PBM_INTERNAL_H

#include "storage/pbm.h"

///-------------------------------------------------------------------------
/// Constants
///-------------------------------------------------------------------------

#define NS_PER_MS 	1000 * 1000			// 10^6 ns per millisecond
#define NS_PER_SEC 	(1000 * NS_PER_MS) 	// 10^9 ns per second

static const long AccessTimeNotRequested = 0;
static const unsigned int PQ_BucketOutOfRange = (unsigned int)(-1);

///-------------------------------------------------------------------------
/// PBM Configuration
///-------------------------------------------------------------------------

/// How much to shift the block # by to find its group
#define BLOCK_GROUP_SHIFT 5 // 10

/// Size of the hash maps
static const long ScanMapMaxSize = 1024;
static const long BlockGroupMapMaxSize = 1024;

/// Clock to use
#define PBM_CLOCK CLOCK_MONOTONIC // CLOCK_MONOTONIC_COARSE

/// PQ configuration
static const int PQ_NumBucketGroups = 10;
static const int PQ_NumBucketsPerGroup = 5;
static const int PQ_NumBuckets = PQ_NumBucketGroups * PQ_NumBucketsPerGroup;
static const int64_t PQ_TimeSlice = 100 * NS_PER_MS;

/// Debugging flags
#define TRACE_PBM
//#define TRACE_PBM_REPORT_PROGRESS
//#define TRACE_PBM_PRINT_SCANMAP
//#define TRACE_PBM_PRINT_BLOCKMAP
//#define TRACE_PBM_BUFFERS
//#define TRACE_PBM_BUFFERS_NEW
//#define TRACE_PBM_BUFFERS_EVICT
#define TRACE_PBM_PQ_REFRESH
#define TRACE_PBM_EVICT

//#define SANITY_PBM_BUFFERS


///-------------------------------------------------------------------------
/// Helper macros & inline functions
///-------------------------------------------------------------------------

/// Helper to make sure locks get freed
/// NOTE: do NOT `return` or `break` from inside the lock guard (break inside nested loop is OK), the lock won't be released.
/// `continue` from the lock guard is OK, will go to the end.
#define LOCK_GUARD_V2(lock, mode) \
  for (bool _c_ = true, pg_attribute_unused() _unused = LWLockAcquire((lock), (mode)); _c_; LWLockRelease((lock)), _c_ = false)
#define EXIT_LOCK_SCOPE continue

/// Group blocks by ID to reduce the amount of metadata required
#define BLOCK_GROUP(block) ((block) >> BLOCK_GROUP_SHIFT)
#define GROUP_TO_FIRST_BLOCK(group) ((group) << BLOCK_GROUP_SHIFT)

/// Convert a timestamp in ns to the corresponding timeslice in the PQ
static inline long ns_to_timeslice(long t) {
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

/// Info about a scan for the block group. (linked list)
typedef struct BlockGroupScanList {
	// Info about the current scan
	ScanId		scan_id;
	BlockNumber	blocks_behind;

	// Next item in the list
	struct BlockGroupScanList* next;
} BlockGroupScanList;

/// Record data for this block group.
typedef struct BlockGroupData {
// ### some kind of lock?

	// Set of scans which care about this block group
	BlockGroupScanList* scans_head;

	// Set of (### unpinned?) buffers holding data in this block group
	int buffers_head;

	// Linked-list in PQ buckets
	struct PbmPQBucket* pq_bucket;
	struct BlockGroupData* bucket_next;
	struct BlockGroupData* bucket_prev;
} BlockGroupData;

typedef struct PbmPQBucket {
	struct BlockGroupData* bucket_head;
	struct BlockGroupData* bucket_tail;
	// ### could also include start & end time (delta) in the bucket?
	// TODO (spin) lock?
} PbmPQBucket;


/// The priority queue structure for tracking blocks to evict
typedef struct PbmPQ {
// protected by PbmPqLock

	PbmPQBucket ** buckets;
	// ### keep track of "not requested" and "very far future" separately?
	PbmPQBucket * not_requested_bucket;
	PbmPQBucket * not_requested_other;
	PbmPQBucket nr1;
	PbmPQBucket nr2;

	_Atomic(long) last_shifted_time_slice;
} PbmPQ;


/// Main PBM data structure
typedef struct PbmShared {
	// TODO more granular locking if it could improve performance.
	// ### where to use `volatile`?

	/// Time-related stuff
	time_t start_time_sec;

	/// Atomic counter for ID generation
	/// Note: protected by PbmScansLock: could make it atomic but it is only accessed when we lock the hash table anyways.
	_Atomic(ScanId) next_id;

	/// Map[ scan ID -> scan stats ] to record progress & estimate speed
	/// Protected by PbmScansLock
	struct HTAB * ScanMap;

// ### Map[ range (table -> block indexes) -> scan ] --- for looking up what scans reference a particular block, maybe useful later
	//struct HTAB * ScansByRange;

	/// Map[ table -> BlockGroup -> set of scans ] + free-list of the list-nodes for tracking statistics on each buffer
	/// Protected by PbmBlocksLock
	struct HTAB * BlockGroupMap;
	BlockGroupScanList* block_group_stats_free_list;
// ### if possible, free list could be protected by its own spinlock instead of using this lock
// TODO modify the map to be: (i.e. linked hash-map of block groups, stored in fixed-size buffers to ammortize allocations and reduce the size of hash map needed)
//   Map[ (table, block group >> N) -> entry ]
//   where N is some chosen constant
//   where entry =  {
//   	Array [ 1 << N ] of BlockGroupData
//		entry* next ptr
//	 }

	/// Priority queue of block groups that could be evicted
	PbmPQ * BlockQueue;

	/// Global estimate of speed used for *new* scans
	/// Currently NOT protected, as long as write is atomic we don't really care about lost updates...
	_Atomic(float) initial_est_speed;


// ### Potential other fields:
// ...
} PbmShared;


///-------------------------------------------------------------------------
/// Global variables
///-------------------------------------------------------------------------

/// Global pointer to the single PBM
extern PbmShared * pbm;

///-------------------------------------------------------------------------
/// PBM PQ Initialization
///-------------------------------------------------------------------------
extern PbmPQ* InitPbmPQ(void);
extern Size PbmPqShmemSize(void);

///-------------------------------------------------------------------------
/// PBM PQ manipulation
///-------------------------------------------------------------------------
extern void PQ_InsertBlockGroup(PbmPQ * pq, BlockGroupData * block_group, long t, bool requested);
extern void PQ_RemoveBlockGroup(BlockGroupData *block_group);
extern bool PQ_ShiftBucketsWithLock(PbmPQ * pq, long t);
extern bool PQ_CheckEmpty(const PbmPQ * pq);
extern struct BufferDesc* PQ_Evict(PbmPQ * pq, uint32 * but_state);

///-------------------------------------------------------------------------
/// Helpers
///-------------------------------------------------------------------------
extern long get_time_ns(void);
extern long get_timeslice(void);


#endif //POSTGRESQL_PBM_INTERNAL_H
