/*
Unit-tests for the calculation to convert a timestamp to an index in the PBM PQ.
*/


#include <stddef.h>
#include <stdio.h>

typedef struct PbmPQ {
    long first_bucket_time;
    long time_slice;
    long buckets_per_group;
} PbmPQ;



// floor(log(x)) for integer values x
unsigned int floor_llogb(unsigned long x) {
	return 8 * sizeof(x) - 1 - __builtin_clzl(x);
}

// calculate the bucket *group* of the given time. Not that each group has several buckets (same # per group)
unsigned int bucket_group(PbmPQ * pq, long t) {
    // time since what should be at the start of the first bucket
    long delta_t = t - pq->first_bucket_time;

	return floor_llogb( 1l + delta_t/(pq->time_slice * pq->buckets_per_group));
}

// determine the bucket index for the given time
unsigned int time_to_bucket(PbmPQ * pq, long t) {
    // special case of things that should already have happened
    if (t <= pq->first_bucket_time) {
        // ### special sentinel for this?
        return 0;
    }

	unsigned int b_group = bucket_group(pq, t);

    // Calculate start time of first bucket in the group: 
	unsigned long group_start_time = pq->first_bucket_time + ((1 << b_group) - 1) * pq->time_slice * pq->buckets_per_group;

    // Calculate width of a bucket within this group:
    unsigned long bucket_width = ((1 << b_group) * pq->time_slice);

    // (t - group_start_time) / bucket_width gives the bucket index *within the group*, then add the bucket index of the first one in the group to get the global bucket index
	size_t bucket_num = b_group * pq->buckets_per_group + (t - group_start_time) / bucket_width;


    // TODO handle case where it would be out of range
    // if (bucket_num > pq->num_buckets) {
    //     ///
    // }

	return bucket_num;
}


/// try to simplify the definition... NEVERMIND, let the compiler do it if it thinks it can... nothing obvious without being able 2^(floor(log(x))) as just x.
unsigned int time_to_bucket2(PbmPQ * pq, long t) {
    const size_t first_bucket_time = pq->first_bucket_time;
    const size_t buckets_per_group = pq->buckets_per_group;
    const long time_slice = pq->time_slice;


    // Special case of things that should already have happened
    if (t <= first_bucket_time) {
        // ### special sentinel for this?
        return 0;
    }

    // Calculate offset from the start of the time range of interest
    const long delta_t = t - first_bucket_time;

    // Calculate the bucket *group* this bucket belongs to
    const unsigned int b_group = floor_llogb(1l + delta_t/(time_slice * buckets_per_group));

    // Calculate start time of first bucket in the group
    const unsigned long group_start_time = first_bucket_time + ((1 << b_group) - 1) * time_slice * buckets_per_group;

    // Calculate index of first bucket in the group
    const unsigned long group_first_bucket_idx = b_group * buckets_per_group;

    // Calculate width of a bucket within this group:
    const unsigned long bucket_width = (1 << b_group) * time_slice;

    // (t - group_start_time) / bucket_width gives the bucket index *within the group*, then add the bucket index of the first one in the group to get the global bucket index
    const size_t bucket_num = group_first_bucket_idx + (t - group_start_time) / bucket_width;

// TODO handle case where it would be out of range

    return bucket_num;
}

void check_bucket(PbmPQ * pq, long t, size_t expect) {
    size_t got = time_to_bucket2(pq, t);
    if (got != expect) {
        printf("ERROR: for time %ld expected %ld but got %ld\n", t, expect, got);
    }
}

void print(PbmPQ * pq, long t) {
    printf("t=%ld:  got %u", t, time_to_bucket(pq,t));
}

#define BCHECK(t, e) check_bucket(&pq, (t), (e))

void check_group(PbmPQ * pq, long t, size_t expect) {
    size_t got = bucket_group(pq, t);
    if (got != expect) {
        printf("ERROR: for time %ld expected group %ld but got %ld\n", t, expect, got);
    }
}

void check_group_range(PbmPQ * pq, long lo, long hi, unsigned int expect) {
    for (long t = lo; t <= hi; ++t) {
        unsigned int got = time_to_bucket2(pq, t);
        if (got != expect) {
            printf("ERROR: for time %ld expected %u but got %u\n", t, expect, got);
            break;
        }
    }
}

#define GCHECK(t, e) check_group(&pq, (t), (e))

#define RCHECK(lo, hi, e) check_group_range(&pq, (lo), (hi), (e))

void test1_bucket(void) {
    PbmPQ pq = {
        .time_slice = 100,
        .buckets_per_group = 3,
        .first_bucket_time = 0,
    };

// groups 0-2 are first group: [0,99] [100,199] [200,299]
// groups 3-5 are second group: [300,499] [500,699] [700,899]
// groups 6-8 are third group: [900,1299] [1300,1699] [1700,2099]
    BCHECK(0, 0);
    BCHECK(99, 0);
    BCHECK(100, 1);
    BCHECK(199, 1);
    BCHECK(200, 2);
    BCHECK(299, 2);
// everything past here is +1 of what it should be
    BCHECK(300, 3);
    BCHECK(350, 3);
    BCHECK(400, 3);
    BCHECK(499, 3);
    BCHECK(500, 4);
    BCHECK(699, 4);
    BCHECK(700, 5);
    BCHECK(899, 5);
    BCHECK(900, 6);
    BCHECK(1299, 6); /// 6 -> 8 here? (+2)
    BCHECK(1300, 7);
    BCHECK(1699, 7); /// 7 -> 9
    BCHECK(1700, 8);
    BCHECK(2099, 8); /// 8 -> 10
    BCHECK(2100, 9);
}


void test1_group(void) {
    PbmPQ pq = {
        .time_slice = 100,
        .buckets_per_group = 3,
        .first_bucket_time = 0,
    };

// groups 0-2 are first group: [0,99] [100,199] [200,299]
// groups 3-5 are second group: [300,499] [500,699] [700,899]
// groups 6-8 are third group: [900,1299] [1300,1699] [1700,2099]
    GCHECK(0, 0);
    GCHECK(99, 0);
    GCHECK(100, 0);
    GCHECK(199, 0);
    GCHECK(200, 0);
    GCHECK(299, 0);
// everything past here is +1 of what it should be
    GCHECK(300, 1);
    GCHECK(350, 1);
    GCHECK(400, 1);
    GCHECK(499, 1);
    GCHECK(500, 1);
    GCHECK(699, 1);
    GCHECK(700, 1);
    GCHECK(899, 1);
    GCHECK(900, 2);
    GCHECK(1299, 2); /// 6 -> 8 here? (+2)
    GCHECK(1300, 2);
    GCHECK(1699, 2);
    GCHECK(1700, 2);
    GCHECK(2099, 2);
    GCHECK(2100, 3);
}



void test2_bucket(void) {
    PbmPQ pq = {
        .time_slice = 10,
        .buckets_per_group = 6,
        .first_bucket_time = 500,
    };


// groups 0-5 are first group:   [500,509] [510,519] [520,529] [530,539] [540,549] [550,559]
    RCHECK(500,509, 0);
    RCHECK(510,519, 1);
    RCHECK(520,529, 2);
    RCHECK(530,539, 3);
    RCHECK(540,549, 4);
    RCHECK(550,559, 5);
// groups 6-11 are second group: [560,579] [580,599] [600,619] [620,639] [640,659] [660,679]
    RCHECK(560,579, 6);
    RCHECK(580,599, 7);
    RCHECK(600,619, 8);
    RCHECK(620,639, 9);
    RCHECK(640,659, 10);
    RCHECK(660,679, 11);
// groups 12-17 are third group: [680,719] [720,759] [760,799] [800,839] [840,879] [880,919]
    RCHECK(680,719, 12);
    RCHECK(720,759, 13);
    RCHECK(760,799, 14);
    RCHECK(800,839, 15);
    RCHECK(840,879, 16);
    RCHECK(880,919, 17);
// groups 18-24 are fourth group: [920,999] [1000,1079] [1080,1159] [1160,1239] [1240,1319] [1320,1399]
    RCHECK(920,999, 18);
    RCHECK(1000,1079, 19);
    RCHECK(1080,1159, 20);
    RCHECK(1160,1239, 21);
    RCHECK(1240,1319, 22);
    RCHECK(1320,1399, 23);
}

void main(void) {
    test1_group();
    test1_bucket();
    test2_bucket();
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