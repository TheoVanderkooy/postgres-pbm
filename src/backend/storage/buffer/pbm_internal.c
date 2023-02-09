/*
 * Internal methods for predictive buffer manager
 */
#include "postgres.h"

#include "storage/pbm/pbm_internal.h"

#include "common/hashfn.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/buf_internals.h"

#include <time.h>


/*-------------------------------------------------------------------------
 * Prototypes for private methods
 *-------------------------------------------------------------------------
 */

#if PBM_USE_PQ
// math functions missing from <math.h>
static inline unsigned int floor_llogb(unsigned long x);


// extra ilist functions
static inline void my_dlist_prepend(dlist_head * list, dlist_head * other);


// initialization functions
static inline void init_pq_bucket(PbmPQBucket* b);


// insert & remove from PQ buckets
static inline BlockGroupData * pq_bucket_pop_front(PbmPQBucket * bucket);
static inline void pq_bucket_remove_locked(BlockGroupData * block_group);
static inline void pq_bucket_push_back_locked(PbmPQBucket * bucket, BlockGroupData * block_group);
static inline void pq_bucket_prepend_range(PbmPQBucket * bucket, PbmPQBucket * other);


// computing what bucket something should go in
static inline long bucket_group_width(unsigned int group);
static unsigned int PQ_time_to_bucket(unsigned long ts);


// PQ bucket locking methods
static inline void pq_bucket_lock(PbmPQBucket * bucket);
static inline void pq_bucket_unlock(PbmPQBucket * bucket);

static inline uint32 pq_bucket_get_null_lock_idx(BlockGroupData * data);
static inline void pq_bucket_null_lock(uint32 lock_idx);
static inline void pq_bucket_null_unlock(uint32 lock_idx);

static inline void pq_bucket_lock_two(PbmPQBucket * b1, PbmPQBucket * b2);
static inline void pq_bucket_unlock_two(PbmPQBucket * b1, PbmPQBucket * b2);


// private debugging functions
static inline void DEBUG_assert_bucket_in_pbm_pq(PbmPQBucket * bucket);
static void DEBUG_print_pq_bucket(StringInfoData * str, PbmPQBucket* bucket);
#endif /* PBM_USE_PQ */

#if PBM_USE_PQ
/*-------------------------------------------------------------------------
 * PBM priority queue initialization methods
 *-------------------------------------------------------------------------
 */

/*
 * Initialization of the PBM priority queue
 */
PbmPQ * InitPbmPQ(void) {
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

	// Initialize the extra bucket locks
	for (int i = 0; i < PQ_NUM_NULL_BUCKETS; ++i) {
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
		SpinLockInit(&pq->null_bucket_locks[i]);
#else
		LWLockInitialize(&pq->null_bucket_locks[i], LWTRANCHE_PBM_PQ_BUCKET);
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK
	}

	return pq;
}

/*
 * Estimate the size of the PBM priority queue
 */
Size PbmPqShmemSize(void) {
	Size size = 0;
	size = add_size(size, sizeof(PbmPQ));
	size = add_size(size, sizeof(PbmPQBucket*) * PQ_NumBuckets);
	size = add_size(size, sizeof(PbmPQBucket) * PQ_NumBuckets);

	return size;
}


/*-------------------------------------------------------------------------
 * PBM priority queue interface with the rest of the PBM
 *-------------------------------------------------------------------------
 */

/*
 * Refresh the block group with current time `t` (ns). Will move the block group
 * from its current bucket (if any) to the new appropriate bucket based on the
 * timestamp (if it is requested).
 *
 * Requires that the block group has buffers and belong in the PQ (otherwise,
 * just call remove instead).
 */
void PQ_RefreshBlockGroup(BlockGroupData *block_group, unsigned long t, bool requested) {
	PbmPQ *const pq = pbm->BlockQueue;
	unsigned int i;
	PbmPQBucket * bucket;
	PbmPQBucket * removed_from;
	uint32 nlock_idx = PQ_NUM_NULL_BUCKETS;

	LOCK_GUARD_V2(PbmPqBucketsLock, LW_SHARED) {
		// Get the appropriate bucket index
		if (requested) {
			i = PQ_time_to_bucket(ns_to_timeslice(t));
		} else {
			i = PQ_BucketOutOfRange;
		}

		// not_requested if not requested or bucket is out of range, otherwise find the bucket
		if (PQ_BucketOutOfRange == i) {
			bucket = pq->not_requested_bucket;
		} else {
			bucket = pq->buckets[i];
		}

		/*
		 * Move it to the new bucket. This requires a loop.
		 */
		for (;;) {
			removed_from = block_group->pq_bucket;

			/* If we want to move it to the same bucket, nothing to do. */
			if (removed_from == bucket) {
				break;
			}

			if (removed_from == NULL) {
				/* NOT currently in a bucket: need some kind of lock to prevent
				 * someone else inserting into a different bucket at the same time
				 *
				 * Note in this case: we always acquire the NULL lock first (and
				 * we never acquire it when releasing) so we don't need to get
				 * both locks to check.
				 */

				// Acquire NULL lock when dealing with a new bucket
				if (nlock_idx >= PQ_NUM_NULL_BUCKETS) {
					nlock_idx = pq_bucket_get_null_lock_idx(block_group);
				}
				pq_bucket_null_lock(nlock_idx);

				// Someone moved it before we got the lock, try again
				if (NULL != block_group->pq_bucket) {
					pq_bucket_null_unlock(nlock_idx);
					continue;
				}

				/* Check that the bucket still has buffers before moving it into
				 * a bucket if it previously wasn't in a bucket.
				 */
				bg_lock_buffers(block_group, LW_SHARED);
				if (FREENEXT_END_OF_LIST == block_group->buffers_head) {
					// NO BUFFERS in the block group, leave it in the "null" bucket!
					// Note: in this case we don't try again, it is in the right spot.
				} else {
					// Insert into new bucket
					pq_bucket_lock(bucket);
					pq_bucket_push_back_locked(bucket, block_group);
				}

				// Release the locks
				bg_unlock_buffers(block_group);
				pq_bucket_null_unlock(nlock_idx);
				pq_bucket_unlock(bucket);
			} else {
				/* Currently in a bucket, need to lock both.
				 *
				 * To avoid deadlocks: we always lock in a deterministic order.
				 * Lock them together before we check if we the bucket changed.
				 */
				pq_bucket_lock_two(removed_from, bucket);

				// Someone removed it before we got the lock, try again
				if (removed_from != block_group->pq_bucket) {
					pq_bucket_unlock_two(removed_from, bucket);
					continue;
				}

				// Move the block group
				pq_bucket_remove_locked(block_group);
				pq_bucket_push_back_locked(bucket, block_group);

				// Release locks and stop
				pq_bucket_unlock_two(removed_from, bucket);
			}

			// If we get here, it is because we didn't `continue` so we succeeded
			break;
		}

	} // LOCK_GUARD
}

/*
 * Remove the specified block group from the PBM PQ when it no longer has any
 * buffers with blocks from this group currently in memory.
 */
void PQ_RemoveBlockGroup(BlockGroupData *const block_group) {
	PbmPQBucket * removed_from = block_group->pq_bucket;
	uint32 nlock_idx;


	/* LOCKING:
	 *  - We do NOT need the shared buckets lock here - we don't care if buckets
	 *    get moved around since we are not inserting.
	 *  - However, it COULD be moved by refresh buckets if it is in bucket[0]
	 *    even with no buffers, so we need to check! (unless we are OK with a
	 *    few empty block groups staying in the PQ, which is probably NBD...)
	 *  - Note: we could just take the shared lock instead for safety, but I don't
	 *    think it is needed
	 *
	 * ### consider taking the shared lock first! to prevent refreshing buckets...
	 * ### but evicting could also take long-running spin locks, so whatever...
	 */

	// Nothing to do if not in a bucket
	if (NULL == removed_from) {
		return;
	}

	/*
	 * Lock the NULL bucket first. Need to lock this to prevent someone from
	 * inserting it into something else while we are removing it.
	 */
	nlock_idx = pq_bucket_get_null_lock_idx(block_group);
	pq_bucket_null_lock(nlock_idx);
	if (NULL == block_group->pq_bucket) {
		// Check again if it was deleted while we acquired the lock
		pq_bucket_null_unlock(nlock_idx);
		return;
	}

/* ### we could also just assume that moved -> someone added something to it.
 * Edge case: gets moved by refresh between removing the buffer and here, but
 * then the next time the block group is non-empty it will get fixed. Not a big
 * deal if a few empty block groups stays in the PQ.
 */

	// Lock the bucket to be removed from -- retry if it changes for safety!
	for (;;) {
		removed_from = block_group->pq_bucket;

		// Somehow got deleted concurrently, we can stop... This should be impossible though!
		if (NULL == removed_from) {
			pq_bucket_null_unlock(nlock_idx);

			elog(WARNING, "Block group got removed from bucket while we had null bucket locked...");
			return;
		}

		pq_bucket_lock(removed_from);

		// Need to make sure it didn't change
		if (removed_from == block_group->pq_bucket) break;

		// Otherwise retry
		pq_bucket_unlock(removed_from);

		// TODO --- this happened repeatedly! a few times per minute with settings:
		//   ./main.py bench -sf 10 -w tpch_c -cm 1 -p 32 -i btree+brin -c dates -sm 128MB
		elog(WARNING, "Block group moved to a different bucket while trying to remove it!");
	}

	/*
	 * We have both current bucket and "null" bucket locked. Make sure nobody
	 * added a buffer while we were getting locks! If so we don't need to
	 * remove anymore, so stop.
	 *
	 * Not sure if necessary, but lock the block group buffers list while we
	 * remove to make this atomic w.r.t. other add/remove ops...
	 *
	 * ### revisit if this is necessary!
	 */
	bg_lock_buffers(block_group, LW_SHARED);
	if (FREENEXT_END_OF_LIST == block_group->buffers_head) {
		/* Remove from the bucket */
		pq_bucket_remove_locked(block_group);
		block_group->pq_bucket = NULL;
	} else {
		/* If there is a buffer now, do nothing. */
	}

	// Unlock
	bg_unlock_buffers(block_group);
	pq_bucket_null_unlock(nlock_idx);
	pq_bucket_unlock(removed_from);
}

/*
 * Shifts the buckets in the PQ if necessary.
 *
 * Assumes the PQ is *already* locked!
 * `ts` is current time *slice*
 *
 * Returns whether it actually did anything. If it returns true, it should be
 * called again since it only shifts one step at a time.
 */
bool PQ_ShiftBucketsWithLock(unsigned long ts) {
	PbmPQ *const pq = pbm->BlockQueue;
	const unsigned long last_shift_ts = pq->last_shifted_time_slice;
	const unsigned long new_shift_ts = last_shift_ts + 1;
	PbmPQBucket *spare;

	// Once we have the lock, re-check whether shifting is needed
	if (new_shift_ts > ts) {
		return false;
	}

	pq->last_shifted_time_slice = new_shift_ts;

	// bucket[0] will be removed, move its stuff to bucket[1] first
	spare = pq->buckets[0];
	pq_bucket_prepend_range(pq->buckets[1], spare);


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

/*
 * Return true if all buckets in the PQ are empty, so the timestamp can be
 * updated without shifting.
 *
 * This requires the lock to already be held!
 */
bool PQ_CheckEmpty(void) {
	const PbmPQ *const pq = pbm->BlockQueue;
	for (int i = 0; i < PQ_NumBuckets; ++i) {
		if (!dlist_is_empty(&pq->buckets[i]->bucket_dlist)) return false;
	}
	return true;
}


/*-------------------------------------------------------------------------
 * PBM PQ public API for making eviction decisions
 *-------------------------------------------------------------------------
 */

#if PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_MULTI

/*
 * Initialize the state used to track eviction. This is called once per eviction
 * before PBM_EvictPages, which may be called multiple times.
 */
void PBM_InitEvictState(PBM_EvictState * state) {
	state->next_bucket_idx = PQ_NumBuckets;
}

/*
 * Choose some buffers for eviction.
 *
 * Requires PbmEvictionLock to be held before calling.
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

			/* Lock bucket */
			pq_bucket_lock(bucket);

			/* Ignore buckets which are empty */
			if (dlist_is_empty(&bucket->bucket_dlist)) {
				pq_bucket_unlock(bucket);
				bucket = NULL;
			}

			/* Advance to the next_bucket index, either for next loop or next call */
			state->next_bucket_idx -= 1;
		}
	}

	/* Didn't find anything, stop here */
	if (NULL == bucket) {
		return;
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

	/* Unlock bucket */
	pq_bucket_unlock(bucket);
}

#elif PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_SINGLE

/*
 * Choose a buffer for eviction from the PBM PQ.
 *
 * The buffer is returned with the header locked and is guaranteed to be
 * evictable (not pinned by anyone else).
 */
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
#endif // PBM_EVICT_MODE
#endif /* PBM_USE_PQ */


/*-------------------------------------------------------------------------
 * PBM PQ public debugging methods (to be removed eventually...)
 *-------------------------------------------------------------------------
 */

/*
 * A bunch of sanity-check assertions for the PBM.
 *  - All block groups are either empty (no buffers) or part of a bucket
 *  - Block group -> bucket pointer is correct (the block group is in the dlist for the bucket)
 *  - All buckets (with block groups) are actually in the PBM somewhere
 *  - All buffers are part of a bucket -- not that if this is called, we should have already checked the free list is empty
 */
void PBM_DEBUG_sanity_check_buffers(void) {
#ifdef SANITY_PBM_BLOCK_GROUPS
	//	elog(LOG, "STARTING BUFFERS SANITY CHECK --- ran out of buffers");

	// Remember which buffers were seen in some block group
	bool saw_all_buffers = true;
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
				DEBUG_assert_bucket_in_pbm_pq(data->pq_bucket);

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
		if (!saw_buffer[i]) {
//			elog(WARNING, "BUFFERS SANITY CHECK --- did not see buffer %d", i);
			saw_all_buffers = false;
		}
	}
	Assert(saw_all_buffers);

	pfree(saw_buffer);

	elog(LOG, "BUFFERS SANITY CHECK --- ran out of buffers, but no assertions failed ????");
#endif // SANITY_PBM_BLOCK_GROUPS
}

/*
 * Print out the whole PBM priority queue state for debugging.
 *
 * This is very expensive, ideally should only be called for fatal errors.
 */
void PBM_DEBUG_print_pbm_state(void) {
	StringInfoData str;
	initStringInfo(&str);

#if PBM_USE_PQ
	appendStringInfo(&str, "\n\tnot_requested:");
	DEBUG_print_pq_bucket(&str, pbm->BlockQueue->not_requested_bucket);
	appendStringInfo(&str, "\n\tother:        ");
	DEBUG_print_pq_bucket(&str, pbm->BlockQueue->not_requested_other);

	// print PBM PQ
	LOCK_GUARD_V2(PbmPqBucketsLock, LW_EXCLUSIVE) {
		for (int i = PQ_NumBuckets - 1; i >= 0; --i) {
			// skip empty buckets!
			if (dlist_is_empty(&pbm->BlockQueue->buckets[i]->bucket_dlist)) continue;
			appendStringInfo(&str, "\n\t %3d:         ", i);
			DEBUG_print_pq_bucket(&str, pbm->BlockQueue->buckets[i]);
		}
	}
#endif /* PBM_USE_PQ */

	ereport(INFO,
			(errmsg_internal("PBM PQ state:"),
					errdetail_internal("%s", str.data)));
	pfree(str.data);

	debug_log_scan_map();
	debug_log_blockgroup_map();
#if PBM_USE_PQ
	debug_log_find_blockgroup_buffers();
#endif /* PBM_USE_PQ */
}


/*-------------------------------------------------------------------------
 * PBM PQ private helper functions
 *-------------------------------------------------------------------------
 */

#if PBM_USE_PQ
/* floor(log_2(x)) for long values x */
unsigned int floor_llogb(unsigned long x) {
	return 8 * sizeof(x) - 1 - __builtin_clzl(x);
	// ### note: this is not portable (GCC only? 64-bit only?)
}

/*
 * Prepend dlist `other` to the front of `list`.
 *
 * After calling, `list` will have all the elements of `other` followed by its
 * own elements, and `other` will now be empty.
 * Assumes `other` is not empty when called and that `list` is either non-empty
 * OR was initialized (i.e. if its next/prev are NULL this may not work properly.
 */
void my_dlist_prepend(dlist_head * list, dlist_head * other) {
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

/* Initialize a PQ bucket */
void init_pq_bucket(PbmPQBucket* b) {
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	SpinLockInit(&b->slock);
#else
	LWLockInitialize(&b->lock, LWTRANCHE_PBM_PQ_BUCKET);
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK
	dlist_init(&b->bucket_dlist);
}

/* Remove the first block group from the PQ bucket */
BlockGroupData * pq_bucket_pop_front(PbmPQBucket * bucket) {
	dlist_node * node;
	BlockGroupData * ret = NULL;

	pq_bucket_lock(bucket);

	// Do nothing if the bucket is empty
	if (!dlist_is_empty(&bucket->bucket_dlist)) {
		// Pop off the dlist
		node = dlist_pop_head_node(&bucket->bucket_dlist);
		ret = dlist_container(BlockGroupData, blist, node);

		// Unset the PQ bucket pointer
		ret->pq_bucket = NULL;
	}

	pq_bucket_unlock(bucket);
	return ret;
}

/*
 * Remove the block group from its bucket, assuming the bucket itself is already locked.
 *
 * Does NOT change the pq_bucket pointer here! This must either be set to a new
 * bucket, or NULL explicitly by the caller.
 */
void pq_bucket_remove_locked(BlockGroupData *const block_group) {
	Assert(block_group != NULL);
	Assert(block_group->pq_bucket != NULL);

	dlist_delete(&block_group->blist);
}

/* Insert into a PQ bucket, assuming the bucket itself is already locked */
void pq_bucket_push_back_locked(PbmPQBucket *bucket, BlockGroupData *block_group) {
	dlist_push_tail(&bucket->bucket_dlist, &block_group->blist);
	block_group->pq_bucket = bucket;
}

/*
 * Merge two PQ buckets.
 *
 * This locks the buckets internally
 */
void pq_bucket_prepend_range(PbmPQBucket *bucket, PbmPQBucket *other) {
	dlist_iter it;

	pq_bucket_lock_two(bucket, other);

	// Nothing to do if the other list is empty
	if (!dlist_is_empty(&other->bucket_dlist)) {

		// Adjust bucket pointer for each block group on the other bucket
		dlist_foreach(it, &other->bucket_dlist) {
			BlockGroupData *data = dlist_container(BlockGroupData, blist, it.cur);

			data->pq_bucket = bucket;
		}

		// Merge elements from `other` to the start of the bucket and clear `other`
		my_dlist_prepend(&bucket->bucket_dlist, &other->bucket_dlist);
	}

	pq_bucket_unlock_two(bucket, other);
}

/* Width of buckets in a bucket group in # of time slices */
long bucket_group_width(unsigned int group) {
	return (1l << group);
}

/*
 * determine the bucket index for the given time
 *
 * `ts` is time *slice*, not in ns
 */
unsigned int PQ_time_to_bucket(const unsigned long ts) {
	// Use "time slice" instead of NS for calculations
	const unsigned long first_bucket_timeslice = pbm->BlockQueue->last_shifted_time_slice;

	// Special case of things that should already have happened
	if (ts <= first_bucket_timeslice) {
		// ### special sentinel for this?
		return 0;
	}

	// New scope to avoid "declaration before use" error...
	{
		// Calculate offset from the start of the time range of interest
		const unsigned long delta_ts = ts - first_bucket_timeslice;

		// Calculate the bucket *group* this bucket belongs to
		const unsigned int b_group = floor_llogb(1l + delta_ts / PQ_NumBucketsPerGroup);

		// Calculate start time of first bucket in the group
		const unsigned long group_start_timeslice =
				first_bucket_timeslice + ((1l << b_group) - 1) * PQ_NumBucketsPerGroup;

		// Calculate index of first bucket in the group
		const unsigned long group_first_bucket_idx = b_group * PQ_NumBucketsPerGroup;

		// Calculate width of a bucket within this group:
		const unsigned long bucket_width = bucket_group_width(b_group);

		// (t - group_start_time) / bucket_width gives the bucket index *within the group*, then add the bucket index of the first one in the group to get the global bucket index
		const size_t bucket_num = group_first_bucket_idx + (ts - group_start_timeslice) / bucket_width;

		// Return a sentinel when it is out of range.
		if (bucket_num >= PQ_NumBuckets) {
			return PQ_BucketOutOfRange;
		}

		return bucket_num;
	}
}


/*-------------------------------------------------------------------------
 * PBM PQ bucket locking methods
 *-------------------------------------------------------------------------
 */

void pq_bucket_lock(PbmPQBucket * bucket) {
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	SpinLockAcquire(&bucket->slock);
#else
	// We never just read the bucket, so always lock exclusively
	LWLockAcquire(&bucket->lock, LW_EXCLUSIVE);
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK
}

void pq_bucket_unlock(PbmPQBucket * bucket) {
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	SpinLockRelease(&bucket->slock);
#else
	LWLockRelease(&bucket->lock);
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK
}

uint32 pq_bucket_get_null_lock_idx(BlockGroupData * data) {
	uint32 k = (uint32) ( (size_t)(data) / (8*sizeof(BlockGroupData)) );
	// ### consider simpler hash function
	return hash_bytes_uint32(k) % PQ_NUM_NULL_BUCKETS;
}

void pq_bucket_null_lock(uint32 lock_idx) {
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	SpinLockAcquire(&pbm->BlockQueue->null_bucket_locks[lock_idx]);
#else
	LWLockAcquire(&pbm->BlockQueue->null_bucket_locks[lock_idx], LW_EXCLUSIVE);
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK
}

void pq_bucket_null_unlock(uint32 lock_idx) {
#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	SpinLockRelease(&pbm->BlockQueue->null_bucket_locks[lock_idx]);
#else
	LWLockRelease(&pbm->BlockQueue->null_bucket_locks[lock_idx]);
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK
}

void pq_bucket_lock_two(PbmPQBucket * b1, PbmPQBucket * b2) {
	// ensure consistent order to avoid deadlocks
	if (b1 > b2) {
		PbmPQBucket * temp = b1;
		b1 = b2;
		b2 = temp;
	}

#ifdef PBM_PQ_BUCKETS_USE_SPINLOCK
	SpinLockAcquire(&b1->slock);
	SpinLockAcquire(&b2->slock);
#else
	LWLockAcquire(&b1->lock, LW_EXCLUSIVE);
	LWLockAcquire(&b2->lock, LW_EXCLUSIVE);
#endif // PBM_PQ_BUCKETS_USE_SPINLOCK
}

void pq_bucket_unlock_two(PbmPQBucket * b1, PbmPQBucket * b2) {
	pq_bucket_unlock(b1);
	pq_bucket_unlock(b2);
}


/*-------------------------------------------------------------------------
 * PBM PQ private helper functions
 *-------------------------------------------------------------------------
 */

/*
 * Check the given PBM PQ bucket is actually in the list somewhere
 */
void DEBUG_assert_bucket_in_pbm_pq(PbmPQBucket * bucket) {
	if (&pbm->BlockQueue->nr1 == bucket) return;
	if (&pbm->BlockQueue->nr2 == bucket) return;

	for (int i = 0; i < PQ_NumBuckets; ++i) {
		if (pbm->BlockQueue->buckets[i] == bucket) return;
	}

	// If we didn't find the bucket, something is wrong!
	AssertState(false);
}

/*
 * Append data about the given bucket to `str`
 */
void DEBUG_print_pq_bucket(StringInfoData * str, PbmPQBucket* bucket) {
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
#endif /* PBM_USE_PQ */

