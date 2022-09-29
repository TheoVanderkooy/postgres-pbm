
#include "postgres.h"

#include "lib/ilist.h"

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
void pq_bucket_remove_locked(BlockGroupData *const block_group);

static inline
void pq_bucket_push_back(PbmPQBucket *bucket, BlockGroupData *block_group);

static inline
void pq_bucket_prepend_range(PbmPQBucket *bucket, PbmPQBucket *other);

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
	dlist_init(&b->bucket_dlist);
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
bool PQ_ShiftBucketsWithLock(const long ts) {
	PbmPQ *const pq = pbm->BlockQueue;
	const long last_shift_ts = pq->last_shifted_time_slice;
	const long new_shift_ts = last_shift_ts + 1;
	PbmPQBucket *spare;

	// Once we have the lock, re-check whether shifting is needed
	if (new_shift_ts > ts) {
		return false;
	}

	pq->last_shifted_time_slice = new_shift_ts;

	// bucket[0] will be removed, move its stuff to bucket[1] first
	spare = pq->buckets[0];
	pq_bucket_prepend_range(pq->buckets[1], spare);
	// TODO locking: may be better to do this one at a time if we need a spinlock


	// Shift each group
	for (unsigned int group = 0; group < PQ_NumBucketGroups; ++group) {
		long next_group_width;

		// Shift the buckets in the group over 1
		for (unsigned int b = 0; b < PQ_NumBucketsPerGroup; ++b) {
			unsigned int idx = group * PQ_NumBucketsPerGroup + b;
			pq->buckets[idx] = pq->buckets[idx + 1];
		}

		// Check if this was the last group to shift or not
		next_group_width = bucket_group_width(group + 1);
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
bool PQ_CheckEmpty(void) {
	const PbmPQ *const pq = pbm->BlockQueue;
	for (int i = 0; i < PQ_NumBuckets; ++i) {
		if (!dlist_is_empty(&pq->buckets[i]->bucket_dlist)) return false;
	}
	return true;
}

/*
 * Refresh the block group with current time `t` (ns). Will move the block group
 * from its current bucket (if any) to the new appropriate bucket based on the
 * timestamp (if it is requested).
 *
 * Requires that the block group has buffers and belong in the PQ (otherwise,
 * just call remove instead).
 */
void PQ_RefreshBlockGroup(BlockGroupData *const block_group, const long t, bool requested) {
	PbmPQ *const pq = pbm->BlockQueue;
	unsigned int i;
	PbmPQBucket * bucket;

	LOCK_GUARD_V2(PbmPqBucketsLock, LW_SHARED) {
		// Get the appropriate bucket index
		if (requested) {
			i = PQ_time_to_bucket(pq, ns_to_timeslice(t));
		} else {
			i = PQ_BucketOutOfRange;
		}

		// not_requested if not requested or bucket is out of range, otherwise find the bucket
		if (PQ_BucketOutOfRange == i) {
			bucket = pq->not_requested_bucket;
		} else {
			bucket = pq->buckets[i];
		}

		if (bucket == block_group->pq_bucket) {
			// Nothing to do if it should go to the same bucket
		} else {
// TODO locking: lock *both* buckets! (doesn't really matter if this stays in the PQ lock)
// ### decide if need to lock both at the same time, or it is OK to release the remove one first.
			// Remove the block group if it is already in a bucket
			if (block_group->pq_bucket != NULL) {
				pq_bucket_remove_locked(block_group);
			}
			// Then push to the new bucket
			pq_bucket_push_back(bucket, block_group);
		}
	}
}

/// Insert into a PQ bucket
void pq_bucket_push_back(PbmPQBucket *bucket, BlockGroupData *block_group) {
// TODO lock the bucket!
	dlist_push_tail(&bucket->bucket_dlist, &block_group->blist);
	block_group->pq_bucket = bucket;
}

/*
 * Prepend dlist `other` to the front of `list`.
 *
 * After calling, `list` will have all the elements of `other` followed by its
 * own elements, and `other` will now be empty.
 * Assumes `other` is not empty when called and that `list` is either non-empty
 * OR was initialized (i.e. if its next/prev are NULL this may not work properly.
 */
static void my_dlist_prepend(dlist_head * list, dlist_head * other) {
	// Assume "other" is non-empty and that we initialized the lists!
	Assert(!dlist_is_empty(other));
	Assert(list->head.next != NULL);

	// change next/prev pointers of other's head/tail respectively to point to "list"
	other->head.prev->next = list->head.next; // set "other" tail's `next` to head of old list
	other->head.next->prev = &list->head; // set "other" head's `prev` to the list head of old list

	// Set old head's prev ptr to the tail of the new segment
	list->head.next->prev = other->head.prev; // set old list head's prev to "other" tail
	list->head.next = other->head.next; // set new head to the "other"'s head

	// Clear the "other" list
	dlist_init(other);
}


/// Merge two PQ buckets
void pq_bucket_prepend_range(PbmPQBucket *bucket, PbmPQBucket *other) {
	dlist_iter it;

	// Nothing to do if the other lits is empty
	if (dlist_is_empty(&other->bucket_dlist)) {
		return;
	}

// TODO locking --- lock the buckets! which one first?? (based on ptr address?)

	// Adjust bucket pointer for each block group on the other bucket
	dlist_foreach(it, &other->bucket_dlist) {
		BlockGroupData * data = dlist_container(BlockGroupData, blist, it.cur);

		data->pq_bucket = bucket;
	}

	// Merge elements from `other` to the start of the bucket and clear `other`
	my_dlist_prepend(&bucket->bucket_dlist, &other->bucket_dlist);

}

/*
 * Remove the block group from its bucket, assuming the bucket itself is already locked.
 */
static inline
void pq_bucket_remove_locked(BlockGroupData *const block_group) {
	Assert(block_group != NULL);
	Assert(block_group->pq_bucket != NULL);

	dlist_delete(&block_group->blist);

	// Clear the links from the block group after unlinking from the rest of the bucket
	block_group->pq_bucket = NULL;
}

/// Remove the specified block group from the PBM PQ
void PQ_RemoveBlockGroup(BlockGroupData *const block_group) {

	// Nothing to do if not in a bucket
	if (NULL == block_group->pq_bucket) {
		return;
	}

// TODO locking: we probably only need to lock the bucket, not the whole list
	LOCK_GUARD_V2(PbmPqBucketsLock, LW_SHARED) {
		pq_bucket_remove_locked(block_group);
	}
}


static inline
BlockGroupData * pq_bucket_pop_front(PbmPQBucket * bucket) {
	dlist_node * node;
	BlockGroupData * ret;

// TODO lock?

	// Return NULL if bucket is empty
	if (dlist_is_empty(&bucket->bucket_dlist)) {
		return NULL;
	}

	// Pop off the dlist
	node = dlist_pop_head_node(&bucket->bucket_dlist);
	ret = dlist_container(BlockGroupData, blist, node);

	// Unset the PQ bucket pointer
	ret->pq_bucket = NULL;

	return ret;
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

#if PBM_EVICT_MODE == PBM_EVICT_MODE_MULTI
void PBM_InitEvictState(PBM_EvictState * state) {
	state->next_bucket_idx = PQ_NumBuckets;
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
	PbmPQBucket* bucket = NULL;
	dlist_iter d_it;

	/*
	 * LOCKING:
	 *  - Need the eviction lock for the entire duration
	 *  - Need shared bucket lock while selecting a bucket, then lock just the bucket
	 */

	LOCK_GUARD_WITH_RES(evict_lock_no_wait, PbmEvictionLock, LW_EXCLUSIVE) {
		/*
		 * If we had to *wait* for the lock, re-check the buffer free list first
		 * because someone else just evicted multiple buffers.
		 */
		if (!evict_lock_no_wait && have_free_buffer()) {
			EXIT_LOCK_SCOPE;
		}

		/*
		 * Determine which bucket to check next. Loop until we find a non-empty
		 * bucket OR have checked everything and decide to give up.
		 */
		LOCK_GUARD_V2(PbmPqBucketsLock, LW_SHARED) {
			while (NULL == bucket && !PBM_EvictingFailed(state)) {

				/* Check not_requested first, and last if we exhaust everything else */
				if (state->next_bucket_idx >= PQ_NumBuckets || state->next_bucket_idx < 0) {
					bucket = pbm->BlockQueue->not_requested_bucket;
				} else {
					bucket = pbm->BlockQueue->buckets[state->next_bucket_idx];
				}

				/* Ignore buckets which are empty */
				if (dlist_is_empty(&bucket->bucket_dlist)) {
					bucket = NULL;
				}

				/* Advance to the next_bucket index, either for next loop or next call */
				state->next_bucket_idx -= 1;
			}

			if (bucket != NULL) {
// TODO locking: lock the bucket if we found one!
			}
		}

		/* Didn't find anything, stop here */
		if (NULL == bucket) {
			EXIT_LOCK_SCOPE;
		}


		/* Loop over block groups in the bucket */
		dlist_foreach(d_it, &bucket->bucket_dlist) {
			BlockGroupData * it = dlist_container(BlockGroupData, blist, d_it.cur);

			BufferDesc* buf;
			uint32 buf_state;
			int buf_id = it->buffers_head;

			/* Loop over buffers in the block group */
			while (buf_id >= 0) {
				buf = GetBufferDescriptor(buf_id);
				buf_state = pg_atomic_read_u32(&buf->state);

				/*
				 * Free the buffer if it isn't pinned.
				 *
				 * We do not invalidate the buffer, just add to the free list.
				 * It can still be used and will be skipped if it gets pinned
				 * before it is removed from the free list.
				 *
				 * Note: we do NOT need to lock the buffer header to add it
				 */
				if (BUF_STATE_GET_REFCOUNT(buf_state) == 0) {
					StrategyFreeBuffer(buf);
				}

				/* Next buffer in the group */
				buf_id = buf->pbm_bgroup_next;
			}
		} // dlist_foreach

// TODO locking: free bucket lock!

	} // PbmEvictionLock
}

#elif PBM_EVICT_MODE == PBM_EVICT_MODE_SINGLE
struct BufferDesc * PQ_Evict(PbmPQ * pq, uint32 * buf_state_out) {
// TODO locking!
/* Actually just in general, this mode is not fully supported and can probably be improved.
 * If re-enabled: need to check:
 *  - Locking!
 *  - make sure the 2 not_requested buckets are handled everywhere
 *  - ...
 */

	int i = PQ_NumBuckets - 1;

	for (;;) {
		// step 1: try to get something from the not_requested bucket
		for (;;) {

			// swap out the not_requested bucket to avoid contention on not_requested...
			PbmPQBucket * temp = pq->not_requested_bucket;
			pq->not_requested_bucket = pq->not_requested_other;
			pq->not_requested_other = temp;

			// Get a block group from the old not_requested bucket. If nothing, go to next loop
			BlockGroupData * data = pq_bucket_pop_front(pq->not_requested_other);
			if (NULL == data) break;

			// Push the block group back onto not requested
			pq_bucket_push_back(pq->not_requested_bucket, data);

			// Found a block group candidate: check all its buffers
			Assert(data->buffers_head >= 0);
			BufferDesc * buf = GetBufferDescriptor(data->buffers_head);
			for (;;) {
				uint32 buf_state = pg_atomic_read_u32(&buf->state);

				// check if the buffer is evictable
				if (BUF_STATE_GET_REFCOUNT(buf_state) == 0) {
					buf_state = LockBufHdr(buf);
					if (BUF_STATE_GET_REFCOUNT(buf_state) == 0) {
						// viable candidate, return it WITH THE HEADER STILL LOCKED!
						// later callback to PBM will handle removing it from the block group...

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

	// if we got here, we can't find anything...
	return NULL;
}
#endif

/*
 * Check the given PBM PQ bucket is actually in the list somewhere
 */
static inline
void assert_bucket_in_pbm_pq(PbmPQBucket * bucket) {
	if (&pbm->BlockQueue->nr1 == bucket) return;
	if (&pbm->BlockQueue->nr2 == bucket) return;

	for (int i = 0; i < PQ_NumBuckets; ++i) {
		if (pbm->BlockQueue->buckets[i] == bucket) return;
	}

	// If we didn't find the bucket, something is wrong!
	AssertState(false);
}

/*
 * A bunch of sanity-check assertions for the PBM.
 *  - All block groups are either empty (no buffers) or part of a bucket
 *  - Block group -> bucket pointer is correct (the block group is in the dlist for the bucket)
 *  - All buckets (with block groups) are actually in the PBM somewhere
 *  - All buffers are part of a bucket -- not that if this is called, we should have already checked the free list is empty
 */
void PBM_sanity_check_buffers(void) {
#ifdef SANITY_PBM_BLOCK_GROUPS
//	elog(LOG, "STARTING BUFFERS SANITY CHECK --- ran out of buffers");

	// Remember which buffers were seen in some block group
	bool * saw_buffer = palloc(NBuffers * sizeof(bool));
	for (int i = 0; i < NBuffers; ++i) {
		saw_buffer[i] = false;
	}

	// check ALL block groups are part of a PBM PQ bucket
	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
		HASH_SEQ_STATUS status;
		BlockGroupHashEntry * entry;

		// Check ALL block groups!
		hash_seq_init(&status, pbm->BlockGroupMap);
		for (;;) {
			entry = hash_seq_search(&status);
			if (NULL == entry) break;

			// Check all block groups in the segment
			for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
				BlockGroupData * data = &entry->groups[i];
				bool found_bg = false;
				dlist_iter it;
				int b, p;

				// Skip if there are no buffers
				if (data->buffers_head == FREENEXT_END_OF_LIST) continue;

				// If there are buffers: there MUST be a bucket!
				Assert(data->pq_bucket != NULL);

				// Make sure the bucket does actually exist in the PQ
				assert_bucket_in_pbm_pq(data->pq_bucket);

				// Verify the block group is in the dlist of the bucket
				dlist_foreach(it, &data->pq_bucket->bucket_dlist) {
					BlockGroupData * bg = dlist_container(BlockGroupData, blist, it.cur);
					if (bg == data) {
						found_bg = true;
						break;
					}
				}
				Assert(found_bg);

				// See what buffers are in the block group
				b = data->buffers_head;
				p = FREENEXT_END_OF_LIST;
				while (b >= 0) {
					BufferDesc * buf = GetBufferDescriptor(b);

					// Remember that we saw the buffer
					saw_buffer[b] = true;

					// Check backwards link is correct
					Assert(buf->pbm_bgroup_prev == p);

					// Next buffer
					p = b;
					b = buf->pbm_bgroup_next;
				}
			}
		}
	}

	// Make sure we saw all the buffers in the PQ somewhere
	for (int i = 0; i < NBuffers; ++i) {
		Assert(saw_buffer[i]);
	}

	pfree(saw_buffer);

	elog(LOG, "BUFFERS SANITY CHECK --- ran out of buffers, but not assertions failed ????");
#endif // SANITY_PBM_BLOCK_GROUPS
}

static
void print_pq_bucket(StringInfoData * str, PbmPQBucket* bucket) {
	// append each block group
	dlist_iter it;
	dlist_foreach(it, &bucket->bucket_dlist) {
		int b;
		BlockGroupData * data = dlist_container(BlockGroupData,blist,it.cur);
		appendStringInfo(str, "  {");

		if (data->buffers_head < 0) {
			appendStringInfo(str, " empty! ");
		}

		// append list of buffers for the block group
		b = data->buffers_head;
		for(;b >= 0;) {
			BufferDesc * buf = GetBufferDescriptor(b);
			appendStringInfo(str, " %d", b);

			b = buf->pbm_bgroup_next;
		}
		appendStringInfo(str, " %d", b);

		appendStringInfo(str, " }");
	}
}

void PBM_print_pbm_state(void) {
	StringInfoData str;
	initStringInfo(&str);

	appendStringInfo(&str, "\n\tnot_requested:");
	print_pq_bucket(&str, pbm->BlockQueue->not_requested_bucket);
	appendStringInfo(&str, "\n\tother:        ");
	print_pq_bucket(&str, pbm->BlockQueue->not_requested_other);

	// print PBM PQ
	LOCK_GUARD_V2(PbmPqBucketsLock, LW_EXCLUSIVE) {
		for (int i = PQ_NumBuckets - 1; i >= 0; --i) {
			appendStringInfo(&str, "\n\t %3d:         ", i);
			print_pq_bucket(&str, pbm->BlockQueue->not_requested_other);
		}
	}

	ereport(INFO,
			(errmsg_internal("PBM PQ state:"),
					errdetail_internal("%s", str.data)));
	pfree(str.data);
}

