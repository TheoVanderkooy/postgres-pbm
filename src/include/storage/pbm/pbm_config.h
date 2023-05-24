#ifndef POSTGRESQL_PBM_CONFIG_H
#define POSTGRESQL_PBM_CONFIG_H


/* ===== CONFIGURATION FOR PBM ===== */

// (move this to pg_config_manual.h later, for now it is here because changing pg_config_manual.h forces *everything* to recompile)
/*
 * What eviction implementation to use:
 *  0: existing clock algorithm w/ "strategies" (default if USE_PBM is disabled)
 *  1: first implementation with only 1 block at a time (might not work anymore!)
 *  2: method that puts still-valid blocks on the free list and lets the normal
 *     mechanism try multiple times to get from the free list
 *  3: don't track a PQ of block groups, instead randomly sample buffers and
 *     pick the best option from the sample
 */
#define PBM_EVICT_MODE_CLOCK 0
#define PBM_EVICT_MODE_PQ_SINGLE 1
#define PBM_EVICT_MODE_PQ_MULTI 2
#define PBM_EVICT_MODE_SAMPLING 3
#ifdef USE_PBM
	//#define PBM_EVICT_MODE PBM_EVICT_MODE_CLOCK
	//#define PBM_EVICT_MODE PBM_EVICT_MODE_PQ_MULTI
	#define PBM_EVICT_MODE PBM_EVICT_MODE_SAMPLING
#else
	#define PBM_EVICT_MODE PBM_EVICT_MODE_CLOCK
#endif

#if (PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_SINGLE) || (PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_MULTI)
	#define PBM_USE_PQ true
	#define PBM_TRACK_STATS false
#else
	#define PBM_USE_PQ false
	#ifdef USE_PBM
		#define PBM_TRACK_STATS true
	#else
		#define PBM_TRACK_STATS false
	#endif
#endif

/*
 * How to track stats for non-requested buffers:
 *  0: track PBM_BUFFER_NUM_RECENT_ACCESSES recent access times for a buffer to
 *     compute average inter-access time
 *  1: exponentially weighted average
 * Potential future alternatives:
 *  - Use exponentially-weighted average of recent inter-access times
 *  - Track count over recent intervals
 */
#define PBM_BUFFER_STATS_MODE_NRECENT 0
#define PBM_BUFFER_STATS_MODE_GEOMETRIC 1
//#define PBM_BUFFER_STATS_MODE PBM_BUFFER_STATS_MODE_NRECENT
#define PBM_BUFFER_STATS_MODE PBM_BUFFER_STATS_MODE_GEOMETRIC

#if PBM_BUFFER_STATS_MODE == PBM_BUFFER_STATS_MODE_NRECENT
	#define PBM_BUFFER_NUM_RECENT_ACCESS 5
#elif PBM_BUFFER_STATS_MODE == PBM_BUFFER_STATS_MODE_GEOMETRIC
	#define PBM_BUFFER_NUM_EXACT 4
#endif

/* PQ and sampling need to know the set of buffers in each group */
#define PBM_TRACK_BLOCKGROUP_BUFFERS (PBM_USE_PQ || (PBM_EVICT_MODE == PBM_EVICT_MODE_SAMPLING))

/* Whether to evict multiple at once with sampling-based PBM */
#define PBM_SAMPLING_EVICT_MULTI

/* Minimum number of average accesses per block to track an index scan */
#define PBM_IDX_MIN_BLOCK_ACCESSES 1.0f

/* Minimum estimated # of rows (per loop) for an index scan to leave markers on
 * visited buffers and potentially detect trailing scans */
#define PBM_TRAILING_IDX_MIN_ROWS 128

/* Number of index scan markers to allow on a single buffer
 * We don't need this to be high: there are many chances for the marker to be
 * seen (every tuple!) and if they are consistently overwritten, then the
 * trailing scans are not defining the lowest time-to-next-access so that info
 * is not all that important. */
#define PBM_IDX_NUM_MARKERS 2

/* Track average eviction time and periodically log it. */
//#define PBM_TRACK_EVICTION_TIME

#endif //POSTGRESQL_PBM_CONFIG_H
