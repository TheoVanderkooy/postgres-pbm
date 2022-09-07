

#include "postgres.h"

#include "storage/pbm_internal.h"

#include "storage/shmem.h"

#include "miscadmin.h"

#include <time.h>
#include <math.h>

/// Constants
const unsigned int OutOfRange = (unsigned int)(-1);


/// Forward declarations
// ### see if any of these should be moved to the header
unsigned int time_to_bucket(const PbmPQ * pq, long t);



/// Initialization
PbmPQ* InitPbmPQ(void) {
	bool found;
	PbmPQ* pq = ShmemInitStruct("Predictive buffer manager PQ", sizeof(PbmPQ), &found);

	// Should not be called if already initialized!
	Assert(!found && IsUnderPostmaster);

	pq->num_groups = 3;
	pq->buckets_per_group = 2;
	pq->time_slice = NS_PER_SEC / 10; // 100 ms for now

	const size_t num_buckets = pq->num_groups * pq->buckets_per_group;
	pq->num_buckets = num_buckets;

	// ### initialize array of buckets
	pq->buckets = ShmemAlloc(sizeof(PbmPQBucket*) * num_buckets);
	for (size_t i = 0; i < num_buckets; ++i) {
		pq->buckets[i] = ShmemAlloc(sizeof(PbmPQBucket));
		*pq->buckets[i] = (PbmPQBucket) {
			.bucket_head = NULL,
		};
	}



	return pq;
}

/// Time range of the given bucket group
static inline long bucket_group_time_width(const PbmPQ *const pq, unsigned int group) {
	return (1l << group) * pq->time_slice;
	// TODO use this elsewhere as well...
}

/// floor(log_2(x)) for long values x
static inline unsigned int floor_llogb(unsigned long x) {
	return 8 * sizeof(x) - 1 - __builtin_clzl(x);
}


/// determine the bucket index for the given time
unsigned int time_to_bucket(const PbmPQ *const pq, const long t) {
	const size_t first_bucket_time = pq->first_bucket_time;
	const size_t buckets_per_group = pq->buckets_per_group;
	const size_t num_buckets = pq->num_buckets;
	const long time_slice = pq->time_slice;

	// Special case of things that should already have happened
	if (t <= first_bucket_time) {
		// ### special sentinel for this?
		return 0;
	}

	// Calculate offset from the start of the time range of interest
	const unsigned long delta_t = t - first_bucket_time;

	// Calculate the bucket *group* this bucket belongs to
	const unsigned int b_group = floor_llogb(1l + delta_t / (time_slice * buckets_per_group));

	// Calculate start time of first bucket in the group
	const unsigned long group_start_time = first_bucket_time + ((1l << b_group) - 1) * time_slice * buckets_per_group;

	// Calculate index of first bucket in the group
	const unsigned long group_first_bucket_idx = b_group * buckets_per_group;

	// Calculate width of a bucket within this group:
	const unsigned long bucket_width = (1 << b_group) * time_slice;

	// (t - group_start_time) / bucket_width gives the bucket index *within the group*, then add the bucket index of the first one in the group to get the global bucket index
	const size_t bucket_num = group_first_bucket_idx + (t - group_start_time) / bucket_width;

	// Return a sentinel when it is out of range.
	// ### we could make this the caller's responsibility instead
	if (bucket_num >= num_buckets) {
		return OutOfRange;
	}

	return bucket_num;
}


void shift_buckets(PbmPQ *const pq, long t) {
	const long last_shift = pq->last_shifted;
	const long new_shift = last_shift + pq->time_slice;
	pq->last_shifted = new_shift;
	// ### locking!!

	// Nothing to do if not enough time has passed
	if (new_shift > t) return;

	// bucket[0] will be removed
	PbmPQBucket * spare = pq->buckets[0];
	struct BlockGroupData * old_first_buckets_block_groups = spare->bucket_head;
	*spare = (PbmPQBucket) {
		.bucket_head = NULL,
	};

	// Shift each group
	for (unsigned int group = 0; group < pq->num_groups; ++group) {
		// Shift the buckets in the group over 1
		for (unsigned int b = 0; b < pq->buckets_per_group; ++b) {
			unsigned int idx = group * pq->buckets_per_group + b;
			pq->buckets[idx] = pq->buckets[idx+1];
		}

		// Check if this was the last group to shift or not
		long next_group_width = bucket_group_time_width(pq, group + 1);
		if (new_shift % next_group_width != 0 || group+1 == pq->num_groups) {
			// The next bucket group will NOT be shifted (or does not exist)
			// use the "spare bucket" to fill the gap left from shifting, then stop
			unsigned int idx = (group+1) * pq->buckets_per_group - 1;
			pq->buckets[idx] = spare;
			break;
		} else {
			// next group will be shifted, steal the first bucket
			unsigned int idx = (group+1) * pq->buckets_per_group - 1;
			pq->buckets[idx] = pq->buckets[idx+1];
		}
	}


	// TODO re-insert the "old_first_buckets_block_groups"
}

/*
 * TODO:
 *  - shifting buckets "left" based on time passed (and knowing *when* they need to be shifted
 *  -
 */