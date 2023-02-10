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
/* Support new features: tracking statistics for unrequested buffers to
 * estimate next access. */
#define PBM_TRACK_STATS true && defined(USE_PBM)
#endif

/*
 * How to track stats for non-requested buffers:
 *  0: track PBM_BUFFER_NUM_RECENT_ACCESSES recent access times for a buffer to
 *     compute average inter-access time
 * Potential future alternatives:
 *  - Use exponentially-weighted average of recent inter-access times
 *  - Track count over recent intervals
 */
#define PBM_BUFFER_STATS_MODE_NRECENT 0
#define PBM_BUFFER_STATS_MODE PBM_BUFFER_STATS_MODE_NRECENT

#if PBM_BUFFER_STATS_MODE == PBM_BUFFER_STATS_MODE_NRECENT
#define PBM_BUFFER_NUM_RECENT_ACCESS 6
#endif

#endif //POSTGRESQL_PBM_CONFIG_H
