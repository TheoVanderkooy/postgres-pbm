/*
Unit-tests for the logic for shifting buckets
*/


#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>


typedef struct PbmPQBucket {
	int i;
} PbmPQBucket;

typedef struct PbmPQ {
	long last_shifted_time_slices;

    PbmPQBucket ** buckets;

} PbmPQ;


static size_t PQ_NumBucketGroups = 10;
static size_t PQ_NumBucketsPerGroup = 5;
static size_t PQ_NumBuckets;
static long PQ_TimeSlice;


/// Time range of the given bucket group
static inline long bucket_group_time_width(const PbmPQ *const pq, unsigned int group) {
	return (1l << group) * PQ_TimeSlice;
}

static inline long ns_to_timeslice(long t) {
	return t / PQ_TimeSlice;
}

void shift_buckets(PbmPQ *const pq, long t) {
    const long ts = ns_to_timeslice(t);
	const long last_shift_ts = pq->last_shifted_time_slices;
	const long new_shift = last_shift_ts + 1;
	// ### locking!!

	// Nothing to do if not enough time has passed
	if (new_shift > ts) return;
	pq->last_shifted_time_slices = new_shift;


	// bucket[0] will be removed
	PbmPQBucket * spare = pq->buckets[0];

	// Shift each group
	for (unsigned int group = 0; group < PQ_NumBucketGroups; ++group) {
		// Shift the buckets in the group over 1
		for (unsigned int b = 0; b < PQ_NumBucketsPerGroup; ++b) {
			unsigned int idx = group * PQ_NumBucketsPerGroup + b;
			pq->buckets[idx] = pq->buckets[idx+1];
		}

		// Check if this was the last group to shift or not
		long next_group_width = bucket_group_time_width(pq, group + 1);
		if (new_shift % next_group_width >= PQ_TimeSlice || group+1 == PQ_NumBucketGroups) {
			// The next bucket group will NOT be shifted (or does not exist)
			// use the "spare bucket" to fill the gap left from shifting
			unsigned int idx = (group+1) * PQ_NumBucketsPerGroup - 1;
			pq->buckets[idx] = spare;
            break;
		} else {
			// next group will be shifted, steal the first bucket
			unsigned int idx = (group+1) * PQ_NumBucketsPerGroup - 1;
			pq->buckets[idx] = pq->buckets[idx+1];
		}
	}
}


PbmPQ * InitPq(long time_slice, long num_groups, long buckets_per_group) {
    PQ_NumBucketGroups = num_groups;
    PQ_NumBucketsPerGroup = buckets_per_group;
    PQ_NumBuckets = num_groups * buckets_per_group;
    PQ_TimeSlice = time_slice;
    PbmPQ * pq = malloc(sizeof(PbmPQ));
    *pq = (PbmPQ){
        .last_shifted_time_slices = 0,
        .buckets = calloc(PQ_NumBuckets, sizeof(PbmPQBucket*)),
    };

    for (int i = 0; i < PQ_NumBuckets; ++i) {
        pq->buckets[i] = malloc(sizeof(PbmPQBucket));
        *(pq->buckets[i]) = (PbmPQBucket){ .i = i, };
    }

    return pq;
}

void print_pq(PbmPQ * pq) {
    for (int i = 0; i < PQ_NumBuckets; ++i) {
        if (pq->buckets[i] == NULL) {
            printf("null|");
        } else {
            printf("  %2d  |", pq->buckets[i]->i);
        }
    }
    printf("\n");
}

void copy_to_array(PbmPQ * pq, int * arr) {
    for (int i = 0; i < PQ_NumBuckets; ++i) {
        arr[i] = pq->buckets[i]->i;
    }
}

void print_arr(int len, int * arr, int * prev) {
    for (int i = 0; i < len; ++i) {
        if (arr[i] == prev[i]) {
            // printf("------|");
            printf("  %2d  |", arr[i]);
            // break; // stop once it stops being different
        } else {
            // printf(" [%2d] |", arr[i]);
            printf("  %2d  |", arr[i]);
        }
    }
    printf("\n");
}


void test1(void) {
    PbmPQ * pq = InitPq(
        /*time slice*/      10, 
        /*# groups*/        5, 
        /*buckets/group*/   3
    );

    printf("---: ");
    print_pq(pq);

    for (int t = 0; t < 200; t += 5) {
        shift_buckets(pq, t);
        printf("%3d: ", t);
        print_pq(pq);
    }
}


void test2(void) {
    PbmPQ * pq = InitPq(
        /*time slice*/      1, 
        /*# groups*/        4, 
        /*buckets/group*/   5
    );
    int len = PQ_NumBuckets;

    int a[20];
    int b[20];

    copy_to_array(pq, a);
    copy_to_array(pq, b);

    int* cur = a;
    int* prev = b;

    printf("---| ");
    print_pq(pq);


    for (int t = 0; t < 150; t += 1) {
        shift_buckets(pq, t);

        int* temp = prev;
        prev = cur;
        cur = temp;
        
        copy_to_array(pq, cur);

        printf("%3d| ", t);
        print_arr(len, cur, prev);
    }
}


void main(void) {
    test2();
}