#ifndef POSTGRESQL_PBM_INTERNAL_H
#define POSTGRESQL_PBM_INTERNAL_H

#include "storage/pbm.h"

///-------------------------------------------------------------------------
/// Constants
///-------------------------------------------------------------------------

#define NS_PER_SEC 1000000000 // 10^9 ns per second

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
static const size_t PQ_NumBucketGroups = 10;
static const size_t PQ_NumBucketsPerGroup = 5;
static const size_t PQ_NumBuckets = PQ_NumBucketGroups * PQ_NumBucketsPerGroup;
static const int64_t PQ_TimeSlice = NS_PER_SEC / 10;

/// Debugging flags
//#define TRACE_PBM
//#define TRACE_PBM_REPORT_PROGRESS
//#define TRACE_PBM_PRINT_SCANMAP
//#define TRACE_PBM_PRINT_BLOCKMAP
//#define TRACE_PBM_BUFFERS
//#define TRACE_PBM_BUFFERS_NEW
//#define TRACE_PBM_BUFFERS_EVICT
//#define SANITY_PBM_BUFFERS


///-------------------------------------------------------------------------
/// Helper macros
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

///-------------------------------------------------------------------------
/// Forward declarations
///-------------------------------------------------------------------------

struct BlockGroupData;

///-------------------------------------------------------------------------
/// Type definitions
///-------------------------------------------------------------------------

// Info about a scan for the block group. (linked list)
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
	int buffers_head;

	// Linked-list in PQ buckets
	struct PbmPQBucket* pq_bucket;
	struct BlockGroupData* bucket_next;
	struct BlockGroupData* bucket_prev;
	// TODO initialize these fields!
} BlockGroupData;

typedef struct PbmPQBucket {
	struct BlockGroupData* bucket_head;
	// TODO could also include start & end time (delta) in the bucket?
	// TODO (spin) lock?
} PbmPQBucket;


typedef struct PbmPQ {
// protected by PbmPqLock

	PbmPQBucket ** buckets;
	// ### keep track of "not requested" and "very far future" separately?

	volatile long last_shifted;
} PbmPQ;

///-------------------------------------------------------------------------
/// PBM PQ Initialization
///-------------------------------------------------------------------------
extern PbmPQ* InitPbmPQ(void);
extern Size PbmPqShmemSize(void);

///-------------------------------------------------------------------------
/// Helpers
///-------------------------------------------------------------------------
extern long get_time_ns(void);


#endif //POSTGRESQL_PBM_INTERNAL_H
