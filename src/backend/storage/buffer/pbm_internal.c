
#include "postgres.h"

#include "storage/pbm/pbm_internal.h"

#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"

#include "storage/buf_internals.h"

#include <time.h>


///-------------------------------------------------------------------------
/// Forward declarations
///-------------------------------------------------------------------------

static unsigned int PQ_time_to_bucket(const PbmPQ * pq, long t);

///-------------------------------------------------------------------------
/// Inline private helper functions
///-------------------------------------------------------------------------

static inline
void pq_bucket_push_back(PbmPQBucket *bucket, BlockGroupData *block_group);

static inline
void pq_bucket_prepend_range(PbmPQBucket *bucket, PbmPQBucket *other);

void pq_bucket_remove(BlockGroupData *block_group);

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

static inline
void init_pq_bucket(PbmPQBucket* b) {
	b->bucket_head = NULL;
	b->bucket_tail = NULL;
}

PbmPQ* InitPbmPQ(void) {
	bool found;
	PbmPQ* pq = ShmemInitStruct("Predictive buffer manager PQ", sizeof(PbmPQ), &found);

	// Should not be called if already initialized!
	Assert(!found && !IsUnderPostmaster);

	// Create the buckets
	pq->buckets = ShmemAlloc(sizeof(PbmPQBucket*) * PQ_NumBuckets);
	for (size_t i = 0; i < PQ_NumBuckets; ++i) {
		pq->buckets[i] = ShmemAlloc(sizeof(PbmPQBucket));
		init_pq_bucket(pq->buckets[i]);
	}
	init_pq_bucket(&pq->nr1);
	init_pq_bucket(&pq->nr2);

	pq->not_requested_bucket = &pq->nr1;
	pq->not_requested_other = &pq->nr2;

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

	// bucket[0] will be removed, move its stuff to bucket[1] first
	PbmPQBucket *spare = pq->buckets[0];
	pq_bucket_prepend_range(pq->buckets[1], spare);
	// TODO locking: may be better to do this one at a time if we need a spinlock


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
void PQ_InsertBlockGroup(PbmPQ *const pq, BlockGroupData *const block_group, const long t, bool requested) {
	unsigned int i;
	PbmPQBucket * bucket;

	// Should not already be in a bucket
	Assert(NULL == block_group->pq_bucket
		&& NULL == block_group->bucket_prev
		&& NULL == block_group->bucket_next);


	LOCK_GUARD_V2(PbmPqLock, LW_SHARED) {
		// Get the appropriate bucket
		if (requested) {
			i = PQ_time_to_bucket(pq, ns_to_timeslice(t));
			bucket = pq->buckets[i];
		} else {
			// not requested bucket
			bucket = pq->not_requested_bucket;
		}

// TODO should this be inside the lock??
		pq_bucket_push_back(bucket, block_group);
	}
}

/// Insert into a PQ bucket
void pq_bucket_push_back(PbmPQBucket *bucket, BlockGroupData *block_group) {
// TODO lock the bucket!
	if (NULL == bucket->bucket_tail) {
		// bucket was empty:
		block_group->bucket_next = NULL;
		block_group->bucket_prev = NULL;
		bucket->bucket_head = block_group;
		bucket->bucket_tail = block_group;
	} else {
		// bucket was not empty:
		block_group->bucket_next = NULL;
		block_group->bucket_prev = bucket->bucket_tail;
		bucket->bucket_tail->bucket_next = block_group;
		bucket->bucket_tail = block_group;
	}

	block_group->pq_bucket = bucket;
}


/// Merge two PQ buckets
void pq_bucket_prepend_range(PbmPQBucket *bucket, PbmPQBucket *other) {
	BlockGroupData * head = other->bucket_head;
	BlockGroupData * tail = other->bucket_tail;

	// either both or neither is null
	Assert((NULL == head) == (NULL == tail));

	// If "other" is empty, nothing to do
	if (NULL == head) {
		return;
	}

	// reset other bucket to empty
	init_pq_bucket(other);

	// adjust the bucket pointer of each
	for (BlockGroupData * it = head; NULL != it; it = it->bucket_next) {
		it->pq_bucket = bucket;
	}

// TODO lock the bucket!
	if (NULL == bucket->bucket_tail) {
		// bucket was empty:
		head->bucket_prev = NULL;
		bucket->bucket_head = head;

		tail->bucket_next = NULL;
		bucket->bucket_tail = tail;
	} else {
		// bucket was not empty:
		// join head <-> tail
		tail->bucket_next = bucket->bucket_head;
		bucket->bucket_head->bucket_prev = tail;

		// update head pointer
		head->bucket_prev = NULL;
		bucket->bucket_head = head;
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
		pq_bucket_remove(block_group);
	}
}

void pq_bucket_remove(BlockGroupData *block_group) {
	BlockGroupData * next = block_group->bucket_next;
	BlockGroupData * prev = block_group->bucket_prev;
// TODO lock the bucket itself!

	// Unlink the block group from the linked list
	if (NULL != next) {
		next->bucket_prev = prev;
	} else {
		// removing tail, so update that
		block_group->pq_bucket->bucket_tail = prev;
	}

	if (NULL != prev) {
		prev->bucket_next = next;
	} else {
		// if no previous, we are removing the head, so update head pointer of the bucket
		block_group->pq_bucket->bucket_head = next;
	}

	// Clear the links from the block group after unlinking from the rest of the bucket
	block_group->pq_bucket = NULL;
	block_group->bucket_prev = NULL;
	block_group->bucket_next = NULL;
}


static inline
BlockGroupData * pq_bucket_pop_front(PbmPQBucket * bucket) {
// TODO lock?

	// Return NULL if bucket is empty
	if (NULL == bucket->bucket_head) {
		return NULL;
	}

	// First group in the bucket will be returned
	BlockGroupData * ret = bucket->bucket_head;
	bucket->bucket_head = ret->bucket_next;

	if (ret->bucket_next != NULL) {
		// Unlink the head from its successor if applicable
		ret->bucket_next->bucket_prev = NULL;
	} else {
		// If no successor, then the bucket tail should be reset too
		bucket->bucket_tail = NULL;
	}

	// Unlink returned group grom the bucket
	ret->bucket_next = NULL;
	ret->bucket_prev = NULL;
	ret->pq_bucket = NULL;
}

/*
 * TODO: alternate implementation
 *  - in freelist: we instead repeatedly try to get from freelist and only ask PBM if that is empty
 *  - PBM will instead take everything from a bucket and put the buffers on the free list (maybe check not pinned first)
 *  	- unless it is already there, or *maybe* it has refcount
 *  - need a "evicting state" which keeps track of last buffer # tried to evict for each loop
 *  - after calling this freelist will try again until we run out of buckets
 *  - don't need to worry about cleanup: the call later will handle unlinking buffers/buckets as needed!
 *  - though may really want to add block group ptr to buffer header... if can fit in 4 bytes! (or separate set of headers)
 */

#if PBM_EVICT_MODE == 2
void PBM_InitEvictState(PBM_EvictState * state) {
	state->next_bucket_idx = PQ_NumBuckets;
//	state->cur_bucket = NULL;
//	state->cur_block_group = NULL;
//	state->cur_buf = NULL;
	// TODO figure out what fields are actually needed!
}


/*
 * Choose some buffers for eviction.
 *
 * This takes the current bucket in the PBM PQ (skipping any empty ones), and for ALL block groups
 * in the bucket it puts ALL the non-pinned buffers in the group on the normal buffer free list.
 *
 * StrategyGetBuffer can handle valid in-use buffers on the free list (ignores them if not pinned)
 * but if everything gets taken before we are able to evict one of the free-list buffers, this gets
 * called again for the next buffer index.
 */
void PBM_EvictPages(PBM_EvictState * state) {
	PbmPQBucket* bucket;
//	bool freed_anything = false;

retry:

	// Check not_requested first
	if (state->next_bucket_idx >= PQ_NumBuckets) {
		bucket = pbm->BlockQueue->not_requested_bucket;
	} else {
		bucket = pbm->BlockQueue->buckets[state->next_bucket_idx];
	}

	// decrement the next_bucket index
	state->next_bucket_idx -= 1;

	// If bucket is empty, try with the next one
	if (bucket->bucket_head == NULL) {
		if (state->next_bucket_idx < 0) {
			// If out of buckets, just return
			return;
		} else {
			// Otherwise, we should start again with the next bucket
			goto retry;
		}
	}

	// loop over the whole bucket and add every buffer in each group to the free list if it isn't pinned
// TODO locking --- ???
	LOCK_GUARD_V2(PbmPqLock, LW_SHARED) {
		// Loop over block groups in the bucket
		for (BlockGroupData * it = bucket->bucket_head; NULL != it; it = it->bucket_next) {
			BufferDesc* buf;
			int buf_id = it->buffers_head;

			// Loop over buffers in the block group
			while (buf_id >= 0) {
				buf = GetBufferDescriptor(buf_id);
				uint32 buf_state = pg_atomic_read_u32(&buf->state);

				// Check if the buffer is pinned and remember if we found anything
				if (BUF_STATE_GET_REFCOUNT(buf_state) == 0) {
//					freed_anything = true;

					// free the buffer (add to free list if not already there)
					StrategyFreeBuffer(buf);
				}

				buf_id = buf->pbm_bgroup_next;
			}
		}
	}

//	// If the bucket didn't have any buffers, go again
//	if (freed_anything) {
//		return;
//	}



}

#elif PBM_EVICT_MODE == 1
// TODO!
struct BufferDesc * PQ_Evict(PbmPQ * pq, uint32 * buf_state_out) {
	// TODO check "not requested" first?
	// TODO locking!

#if defined(TRACE_PBM) && defined(TRACE_PBM_EVICT)
	static int x = 0;
	int y = x;
	++x;
	elog(LOG, "PQ_Evict (%d) START", y);
#endif // TRACE_PBM && TRACE_PBM_EVICT

	int i = PQ_NumBuckets - 1;

	for (;;) {
		// step 1: try to get something from the not_requested bucket
		for (;;) {
#if defined(TRACE_PBM) && defined(TRACE_PBM_EVICT)
			elog(LOG, "PQ_Evict (%d) (i=%d) CHECK NOT_REQUESTED", y, i);
#endif // TRACE_PBM && TRACE_PBM_EVICT

			// swap out the not_requested bucket to avoid contention on not_requested...
			PbmPQBucket * temp = pq->not_requested_bucket;
			pq->not_requested_bucket = pq->not_requested_other;
			pq->not_requested_other = temp;

			// Get a block group from the old not_requested bucket. If nothing, go to next loop
			BlockGroupData * data = pq_bucket_pop_front(pq->not_requested_other);
			if (NULL == data) break;

			// Push the block group back onto not requested
			pq_bucket_push_back(pq->not_requested_bucket, data);

#if defined(TRACE_PBM) && defined(TRACE_PBM_EVICT)
			elog(LOG, "PQ_Evict (%d) (i=%d) found candidate block group, check its buffers", y, i);
#endif // TRACE_PBM && TRACE_PBM_EVICT

			// Found a block group candidate: check all its buffers
			Assert(data->buffers_head >= 0);
			BufferDesc * buf = GetBufferDescriptor(data->buffers_head);
			for (;;) {
				uint32 buf_state = pg_atomic_read_u32(&buf->state);

				// check if the buffer is evictable
// ### check usagecount?
				if (BUF_STATE_GET_REFCOUNT(buf_state) == 0) {
					buf_state = LockBufHdr(buf);
					if (BUF_STATE_GET_REFCOUNT(buf_state) == 0) {
						// viable candidate, return it WITH THE HEADER STILL LOCKED!
						// later callback to PBM will handle removing it from the block group...

#if defined(TRACE_PBM) && defined(TRACE_PBM_EVICT)
						elog(LOG, "PQ_Evict (%d) END_SUCCESS", y);
#endif // TRACE_PBM && TRACE_PBM_EVICT

						*buf_state_out = buf_state;
						return buf;
					}
					UnlockBufHdr(buf, buf_state);
				}

				// If buffer isn't evictable, get the next one
				if (buf->pbm_bgroup_next < 0) {
					// out of buffers in this group!
					break;
				} else {
					buf = GetBufferDescriptor(buf->pbm_bgroup_next);
				}
			}
		}

		// if we checked all the buckets, nothing to return...
		if (i < 0) {
			break;
		}

		// step 2: if nothing in the not_requested bucket, move current bucket to not_requested and try again
	// ### maybe move one at a time instead? only decrement `i` when bucket is empty
		pq_bucket_prepend_range(pq->not_requested_bucket, pq->buckets[i]);
		i -= 1;
	}

#if defined(TRACE_PBM) && defined(TRACE_PBM_EVICT)
	elog(LOG, "PQ_Evict (%d) END_FAIL", y);

	elog(LOG, "PQ_Evict (%d) BUCKETS:", y);
	elog(LOG, "\tnr1={head=%p, tail=%p}", pq->nr1.bucket_head, pq->nr1.bucket_tail);
	elog(LOG, "\tnr2={head=%p, tail=%p}", pq->nr2.bucket_head, pq->nr2.bucket_tail);
	for (int a = 0; a < PQ_NumBuckets; ++a) {
		elog(LOG, "\t%d={head=%p, tail=%p}", a, pq->buckets[a]->bucket_head, pq->buckets[a]->bucket_tail);
	}
#endif // TRACE_PBM && TRACE_PBM_EVICT

	// if we got here, we can't find anything...
	return NULL;
}
#endif

/*
 * TODO:
 *  - hook it up to everything else...
 */