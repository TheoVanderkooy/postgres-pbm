
#include "postgres.h"

#include "storage/pbm_internal.h"

#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"

#include <time.h>


///-------------------------------------------------------------------------
/// Forward declarations
///-------------------------------------------------------------------------

static unsigned int PQ_time_to_bucket(const PbmPQ * pq, long t);

///-------------------------------------------------------------------------
/// Inline private helper functions
///-------------------------------------------------------------------------

/// Width of buckets in a bucket group in # of time slices
static inline long bucket_group_width(unsigned int group) {
	return (1l << group);
}

/// With of buckets in the given bucket group in nanoseconds
static inline long bucket_group_time_width(unsigned int group) {
	return bucket_group_width(group) * PQ_TimeSlice;
}

/// floor(log_2(x)) for long values x
static inline unsigned int floor_llogb(unsigned long x) {
	return 8 * sizeof(x) - 1 - __builtin_clzl(x);
	// ### note: this is not portable (GCC only? 64-bit only?)
}

///-------------------------------------------------------------------------
/// Initialization
///-------------------------------------------------------------------------

PbmPQ* InitPbmPQ(void) {
	bool found;
	PbmPQ* pq = ShmemInitStruct("Predictive buffer manager PQ", sizeof(PbmPQ), &found);

	// Should not be called if already initialized!
	Assert(!found && !IsUnderPostmaster);

	// Create the buckets
	pq->buckets = ShmemAlloc(sizeof(PbmPQBucket*) * PQ_NumBuckets);
	for (size_t i = 0; i < PQ_NumBuckets; ++i) {
		pq->buckets[i] = ShmemAlloc(sizeof(PbmPQBucket));
		*pq->buckets[i] = (PbmPQBucket) {
			.bucket_head = NULL,
		};
	}

	return pq;
}

Size PbmPqShmemSize(void) {
	Size size = 0;
	size = add_size(size, sizeof(PbmPQ));
	size = add_size(size, sizeof(PbmPQBucket*) * PQ_NumBuckets);
	size = add_size(size, sizeof(PbmPQBucket) * PQ_NumBuckets);

	return size;
}

///-------------------------------------------------------------------------
/// PBM PQ helper functions
///-------------------------------------------------------------------------

/// determine the bucket index for the given time
/// `ts` is time *slice*, not in ns
static unsigned int PQ_time_to_bucket(const PbmPQ *const pq, const long ts) {
	// Use "time slice" instead of NS for calculations
	const long first_bucket_timeslice = pq->last_shifted_time_slice;

	// Special case of things that should already have happened
	if (ts <= first_bucket_timeslice) {
		// ### special sentinel for this?
		return 0;
	}

	// Calculate offset from the start of the time range of interest
	const unsigned long delta_ts = ts - first_bucket_timeslice;

	// Calculate the bucket *group* this bucket belongs to
	const unsigned int b_group = floor_llogb(1l + delta_ts / PQ_NumBucketsPerGroup);

	// Calculate start time of first bucket in the group
	const unsigned long group_start_timeslice = first_bucket_timeslice + ((1l << b_group) - 1) * PQ_NumBucketsPerGroup;

	// Calculate index of first bucket in the group
	const unsigned long group_first_bucket_idx = b_group * PQ_NumBucketsPerGroup;

	// Calculate width of a bucket within this group:
	const unsigned long bucket_width = bucket_group_width(b_group);

	// (t - group_start_time) / bucket_width gives the bucket index *within the group*, then add the bucket index of the first one in the group to get the global bucket index
	const size_t bucket_num = group_first_bucket_idx + (ts - group_start_timeslice) / bucket_width;

	// Return a sentinel when it is out of range.
	// ### we could make this the caller's responsibility instead
	if (bucket_num >= PQ_NumBuckets) {
		return PQ_BucketOutOfRange;
	}

	return bucket_num;
}


///-------------------------------------------------------------------------
/// PBM PQ Public API
///-------------------------------------------------------------------------

/// Shifts the buckets in the PQ if necessary.
/// Assumes the PQ is *already* locked!
/// `ts` is current time *slice*
bool PQ_ShiftBucketsWithLock(PbmPQ *pq, long ts) {
	const long last_shift_ts = pq->last_shifted_time_slice;
	const long new_shift_ts = last_shift_ts + 1;

	// Once we have the lock, re-check whether shifting is needed
	if (new_shift_ts > ts) {
		return false;
	}

	pq->last_shifted_time_slice = new_shift_ts;

	// bucket[0] will be removed
	PbmPQBucket *spare = pq->buckets[0];
	struct BlockGroupData *old_first_buckets_block_groups = spare->bucket_head;
	*spare = (PbmPQBucket) {
			.bucket_head = NULL,
	};

	// Shift each group
	for (unsigned int group = 0; group < PQ_NumBucketGroups; ++group) {
		// Shift the buckets in the group over 1
		for (unsigned int b = 0; b < PQ_NumBucketsPerGroup; ++b) {
			unsigned int idx = group * PQ_NumBucketsPerGroup + b;
			pq->buckets[idx] = pq->buckets[idx + 1];
		}

		// Check if this was the last group to shift or not
		long next_group_width = bucket_group_width(group + 1);
		if (new_shift_ts % next_group_width != 0 || group + 1 == PQ_NumBucketGroups) {
			// The next bucket group will NOT be shifted (or does not exist)
			// use the "spare bucket" to fill the gap left from shifting, then stop
			unsigned int idx = (group + 1) * PQ_NumBucketsPerGroup - 1;
			pq->buckets[idx] = spare;
			break;
		} else {
			// next group will be shifted, steal the first bucket
			unsigned int idx = (group + 1) * PQ_NumBucketsPerGroup - 1;
			pq->buckets[idx] = pq->buckets[idx + 1];
		}
	}

// ### re-insert the "old_first_buckets_block_groups" (after the lock?)
// ### if keeping this: add a tail pointer in the bucket too? Or make the list circular?
	// FOR NOW: just add them to the new first bucket...
	if (old_first_buckets_block_groups != NULL) {
		PbmPQBucket * bucket0 = pq->buckets[0];
		if (NULL == bucket0->bucket_head) {
			bucket0->bucket_head = old_first_buckets_block_groups;
		} else {
			BlockGroupData *old_head = old_first_buckets_block_groups;
			BlockGroupData *old_tail = old_first_buckets_block_groups;
			// find tail of the old first bucket list
			while (old_tail->bucket_next != NULL) {
				old_tail = old_tail->bucket_next;
			}
			// link at the start of the new first bucket's list
			old_tail->bucket_next = bucket0->bucket_head;
			bucket0->bucket_head->bucket_prev = old_tail;
			bucket0->bucket_head = old_head;
		}
	}

	return true;
}

/// Return true if all buckets in the PQ are empty, so the timestamp can be updated without shifting
/// This requires the lock to already be held!
bool PQ_CheckEmpty(const PbmPQ *pq) {
	for (int i = 0; i < PQ_NumBuckets; ++i) {
		if (pq->buckets[i]->bucket_head != NULL) return false;
	}
	return true;
}

/// Insert a block group into the PBM PQ with current time `t` (ns)
void PQ_InsertBlockGroup(PbmPQ *const pq, BlockGroupData *const block_group, const long t) {
	unsigned int i;
	PbmPQBucket * bucket;

	// Should not already be in a bucket
	Assert(NULL == block_group->pq_bucket
		&& NULL == block_group->bucket_prev
		&& NULL == block_group->bucket_next);


	LOCK_GUARD_V2(PbmPqLock, LW_SHARED) {
		// Get the appropriate bucket
		i = PQ_time_to_bucket(pq, ns_to_timeslice(t));
		bucket = pq->buckets[i];

// TODO lock bucket for insert!
		// Insert into the bucket
		block_group->pq_bucket = bucket;
		block_group->bucket_next = bucket->bucket_head;
		bucket->bucket_head = block_group;
	}
}

/// Remove the specified block group from the PBM PQ
void PQ_RemoveBlockGroup(BlockGroupData *block_group) {
	BlockGroupData * next = block_group->bucket_next;
	BlockGroupData * prev = block_group->bucket_prev;

	// Nothing to do if not in a bucket
	if (NULL == block_group->pq_bucket) {
		Assert(NULL == block_group->bucket_prev && NULL == block_group->bucket_next);
		return;
	}

// ### is locking the whole thing actually necessary?
	LOCK_GUARD_V2(PbmPqLock, LW_SHARED) {
// TODO lock the bucket itself!

		// Unlink the block group from the linked list
		if (NULL != next) {
			next->bucket_prev = prev;
		}
		if (NULL != prev) {
			prev->bucket_next = next;
		} else {
			// if no previous, we are removing the head, so update head pointer of the bucket
			block_group->pq_bucket->bucket_head = next;
		}
	}

	// Clear the links from the block group after unlinking from the rest of the bucket
	block_group->pq_bucket = NULL;
	block_group->bucket_prev = NULL;
	block_group->bucket_next = NULL;
}


/*
 * TODO:
 *  - hook it up to everything else...
 */