#ifndef POSTGRESQL_PBM_INTERNAL_H
#define POSTGRESQL_PBM_INTERNAL_H

/// Constants

#define NS_PER_SEC 1000000000


/// Configuration (### ?)


/// Forward declarations
struct BlockGroupData;


/// Type definitions


typedef struct PbmPQBucket {
	struct BlockGroupData* bucket_head;
	// TODO could also include start & end time (delta) in the bucket?
} PbmPQBucket;


typedef struct PbmPQ {
/// protected by PbmPqLock
// TODO move these as global constants? (maybe only need to be in the .c file)
	size_t num_groups;
	size_t buckets_per_group;
	size_t num_buckets; // = num_groups * buckets_per_group
	long time_slice;

	/// Total number of buckets is num_groups * buckets_per_group

	PbmPQBucket ** buckets;
	// ### keep track of "not requested" and "very far future" separately?

	long last_shifted;
	long first_bucket_time;





} PbmPQ;


/// Initialization
extern PbmPQ* InitPbmPQ(void);


extern long get_time_ns(void);


#endif //POSTGRESQL_PBM_INTERNAL_H
