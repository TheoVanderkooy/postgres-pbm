/*
 * TODO theo
 */
#include "postgres.h"

#include "access/heapam.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/pbm.h"
#include "miscadmin.h"


#define TRACE_PBM


/*-------------------------------------------------------------------------
 * Forward declarations of internals:
 *-------------------------------------------------------------------------
 */


typedef ScanId ScansTag;

typedef struct {
	// - *info about the scan*
	// - last progress update time
	// - current position (# tuples?)
	// - estimate of speed -- use geometric weighted average?
	struct timeval last_report_time; // TODO what type to use for time?
	// TODO last reported (block?)
} ScanData;

typedef struct {
	// Hash key
	ScansTag tag;

	// Value
	ScanData data;
} ScansHashEntry;


typedef struct {
	ScanId scan_id;
	size_t tuples_behind;
} ScanStatusPair;

typedef struct {
//	LWLock lock;
	// Protected by `

	// TODO size_t or uint32_t? (atomic u32 would be nice for just generating IDs, but I think we need other book-keeping on register-scan too)
	ScanId next_id;

	HTAB * ScanMap;

	// Array[NBlocks] of SET of (scan id, tuples behind) (or map?)

	/*
	 * TODO other fields:
	 *  - LWLock or spinlock?
	 *  	- BufferAccessStrategy uses a spinlock.
	 *  	- Each buffer slot header has a LWLock protecting the data in the buffer
	 *  - ...
	 */
} PbmShared;

PbmShared* pbm;

int *x;
int **y;


/*-------------------------------------------------------------------------
 * Initialization:
 *-------------------------------------------------------------------------
 */
void InitPBM(void) {
	bool found;
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

	hash_info = (HASHCTL){
		.keysize = sizeof(ScansTag),
		.entrysize = sizeof(ScansHashEntry),
		.num_partitions = 16, // TODO what to pick? partition at all? how is NUM_BUFFER_PARTITIONS used?
	};
	int hash_flags = HASH_ELEM | HASH_BLOBS | HASH_PARTITION;
	pbm->ScanMap = ShmemInitHash("PBM active scan stats", 128, 1024, &hash_info, hash_flags);

	// TODO initialize other fields
	// TODO make sure to update PbmShmemSize for new allocations here

//	x = ShmemAlloc(sizeof(int));
}


Size PbmShmemSize(void) {
	Size size = 0;

	size = add_size(size, sizeof(PbmShared));

	// TODO


	return size;
}

/*-------------------------------------------------------------------------
 * Public API:
 *-------------------------------------------------------------------------
 */


#define LOCK_GUARD(lock, mode, code) LWLockAcquire((lock), (mode)); code LWLockRelease((lock));

#define LOCK_GUARD_V2(lock, mode) \
  for (bool _c_ = true, _ = LWLockAcquire((lock), (mode)); _c_; LWLockRelease((lock)), _c_ = false)


// TODO may want to pass in "ScanKey" too? what does that do?
void RegisterSeqScan(HeapScanDesc scan/*TODO theo args -- table, [columns-n/a], [ranges?]*/) {
	ScanId id;

//	LOCK_GUARD(PbmLock, LW_EXCLUSIVE,
//		id = pbm->next_id;
//		pbm->next_id += 1;
//	)


	LOCK_GUARD_V2(PbmLock, LW_EXCLUSIVE)
	{
		id = pbm->next_id;
		pbm->next_id += 1;
	}

//	LWLockAcquire(PbmLock, LW_EXCLUSIVE);
//
//	// Assign unique ID to the scan
//	id = pbm->next_id;
//	pbm->next_id += 1;
//
//
//// NOTE: we CANNOT store the HeapScanDesc directly in the hash map, since it is shared! ... unless we only care about it locally?
//// TODO can we have separate (local) maps for tracking scans??
//
//	LWLockRelease(PbmLock);

	// Scan remembers the ID
	scan->scanId = id;

	// debugging
#ifdef TRACE_PBM
	elog(INFO, "RegisterSeqScan(%d): name=%s, start_block=%d", id, &scan->rs_base.rs_rd->rd_rel->relname.data, scan->rs_startblock);
#endif
	/*
	* 1. Generate ID for the scan (auto-imcrement counter) (returned and/or store in the scan state)
	* 2. For the range(s) specified: store the list of pages --- is this always possible?
	* 3. Register the scan ID and # of tuples that will be read first for each page (all pages or only the ones already in memory?)
	* 4. Re-calculate priorities for relevant pages
	*/
}

void UnregisterScan(ScanId id) {
	/*
	 * Free metadata...
	 */
// TODO make sure this *always* gets called --- check where HeapScanDesc is *deleted* and make sure it is called there!
// Does NOT always get called because the scan doesn't always run to completion -- e.g. limits.
// (might be worth tracking when it is or isn't -- if it did complete there may be less metadata to clean up)

#ifdef TRACE_PBM
	elog(INFO, "UnregisterScan(%d)", id);
#endif
}

void ReportScanPosition(/*TODO theo args -- ??? scan operator should be good enough*/) {
	/*
	 * Update position/speed of the scan
	 */
}

/*-------------------------------------------------------------------------
 * Private internal methods:
 *-------------------------------------------------------------------------
 */


// TODO...

void/*timestamp*/ PageNextConsumption(/*TODO args --- page*/) {
	/*
	 * 1. For all scans of the page: estimate time based on speed and distance from the tuple
	 * 2. Take the minimum as next access time, return that value
	 */
}

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


/*
 * TODO ALSO NEED:
 *  - un-pin page needs to know if we're done with that block, move to not-requested as appropriate
 *  - needs to re-calculate priority regardless
 */


/*
 * TODO:
 *  - figure out shared memory across processes... need to share the data structures! --- see shmem.h/.c
 *  - actually implement stuff!
 */

/*
 * TODO data structures:
 *  - atomic counter for generating scan IDs
 *  - Map scan ID -> stats{ position, speed, ... }
 *  - Map Block ID -> (Map scan ID, "tuples_behind")
 *  	- I think this can just be an array - each buffer has a "buf ID" starting at 0
 *  - Chain of buckets (array)
 *  - Map (funcition) from time distance -> bucket # (may be purely mathematical...
 */


/*
 * NOTE:
 *  - page size = 8KiB (8192 B)
 */