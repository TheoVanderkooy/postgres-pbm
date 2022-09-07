

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
	const unsigned long group_start_time = first_bucket_time + ((1 << b_group) - 1) * time_slice * buckets_per_group;

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


// where f = first_bucket_time, d = time_slice, m = buckets_per_group
// First (#groups) buckets are [f, f+d), [f+d, f+2d), ... [f+(m-1)d, f+md)
// Second (#groups) buckets: [f+md, f+md+2d), ...,  [f+(3m-2)d, f+3md)
// Third (#groups) buckets: [f+3md, f+3md+4d), ..., [f+(7m-4)d, f+7md)
// Fourth (#groups) buckets: [f+7md, f+7md+8d), ..., [f+(15m-8)d, f+15md)

// Bucket group N: (starting at N=1)
// left: f + (2^(N-1)-1) * d				// 0, 1, 3, 7 = 2^(N-1) - 1
// right: f + (2^N - 1) * d					// 1, 3, 7, 15 = 2^N - 1


// f + (2^(N-1) - 1) * md <=   t                <   f + (2^N - 1) * md
// 2^(N-1)                <=  1 + (t-f)/md       <   2^N
// N-1                    <=  log(1 + (t-f)/md)  <   N

// THEREFORE: bucket group # = N = 1 + FLOOR(log(1+(t-f)/md)) ... starting at N=1!
// With N=0 as start: N = FLOOR(log(1+(t-f)/md))


// Bucket group N: (starting at N=0)
// left: f + (2^N - 1) * md					// 0, 1, 3, 7 = 2^(N-1) - 1
// right: f + (2^(N+1) - 1) * md				// 1, 3, 7, 15 = 2^N - 1
// get N <= log(...) < N+1
// THEREFORE: N = FLOOR(log(1+(t-f)/md))




// a-1 <= b < a,  a integer   <=>   a = FLOOR(b) + 1


//... if b = a-1 then CEIL(b) = FLOOR(b) = a-1
//... if a > b > a-1 then FLOOR(b) = a-1,   CEIL(b) = a

// a-1 = floor(b) + 1 - 1 = floor(b) <= b < floor(b) + 1 = a

//    2^(N-1) = 1, 2, 4, 8, 16    2^(N-1) - 1 = 0, 1, 3, 7


/// given bucket number:
// N: f + (2^N - 1) * md <= t < f + (2^(N+1) - 1) * md
// m = # bucket groups
// compute start = s = f + (2^N - 1) * md
// bucket width = 2^N * d

// (f - s) / (1<<N * d) === bucket within group

// Therefore global bucket index =
// N * m +  (t - s) / (2^N * d)
// WHERE
//    N = FLOOR(log(1+(t-f)/md))
//    s = t + (2^N - 1) * md

// N * m + (t - f - ((2^N - 1) * md)) / (2^N * d)
// Nm + (t-f)/(2^N * d) - ((2^N - 1) * md)/(2^N * d)
// Nm + (t-f)/(2^N * d) - (1 - 1/2^N) * m



/*
 * TODO:
 *  - time -> bucket translation
 *  - shifting buckets "left" based on time passed (and knowing *when* they need to be shifted
 *  -
 */