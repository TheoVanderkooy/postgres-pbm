#ifndef POSTGRESQL_PBM_INTERNAL_H
#define POSTGRESQL_PBM_INTERNAL_H

#include "lib/ilist.h"

#include "storage/pbm.h"
#include "storage/relfilenode.h"

///-------------------------------------------------------------------------
/// Constants
///-------------------------------------------------------------------------

#define NS_PER_MS 	(1000 * 1000)		// 10^6 ns per millisecond
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
static const long BlockGroupMapMaxSize = (1 << 16);

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
#define SANITY_PBM_BLOCK_GROUPS


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

/// Block group hash table segment:
#define LOG_BLOCK_GROUP_SEG_SIZE 8
#define BLOCK_GROUP_SEG_SIZE (1 << LOG_BLOCK_GROUP_SEG_SIZE)
#define BLOCK_GROUP_SEGMENT(group) ((group) >> LOG_BLOCK_GROUP_SEG_SIZE)

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

// Table identifier. Stored in the scan map
typedef struct TableData {
	RelFileNode	rnode; // physical relation
	ForkNumber	forkNum; // fork in the relation
} TableData;

/// Structs for storing information about scans in the hash map
// Hash map key
typedef ScanId ScanTag;

// Hash map value: scan info & statistics
// Note: records *blocks* NOT block *groups*
typedef struct ScanData {
	// Scan info (does not change after being set)
	TableData 	tbl; // the table being scanned
	BlockNumber startBlock; // where the scan started
	BlockNumber	nblocks; // # of blocks (not block *groups*) in the table

// ### add range information (later when looking at BRIN indexes)

	// Statistics
// ### Consider concurrency control for these. Only *written* from one thread, but could be read concurrently from others
	long		last_report_time;
	BlockNumber	last_pos;
	BlockNumber	blocks_scanned;
	float		est_speed;
} ScanData;

// Entry (KVP) in the scans hash map
typedef struct ScanHashEntry {
	ScanTag		tag; // Hash key
	ScanData	data; // Value
} ScanHashEntry;


/// Structs for information about block groups

// Info about a scan for the block group. (linked list)
typedef struct BlockGroupScanList {
	// Info about the current scan
	ScanId		scan_id;
	BlockNumber	blocks_behind;

	// Next item in the list
	// ### consider using ilist.h for this! (this seems to not be broken --- don't touch for now)
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
	BlockGroupData		groups[BLOCK_GROUP_SEG_SIZE];
} BlockGroupHashEntry;


/// The priority queue structure data structures for tracking blocks to evict
typedef struct PbmPQBucket {
	// List of BlockGroupData in the list
	dlist_head bucket_dlist;
	// TODO (spin) lock?
} PbmPQBucket;

// The actual PQ structure
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


	/// Map[ (table, BlockGroupSegment) -> set of scans ] + free-list of the list-nodes for tracking statistics on each buffer
	/// Protected by PbmBlocksLock
	struct HTAB * BlockGroupMap;
	BlockGroupScanList* block_group_stats_free_list;
// ### if possible, free list could be protected by its own spinlock instead of using this lock

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
