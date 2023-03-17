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
#define PBM_TRACK_STATS false
#endif

/* PQ and sampling need to know the set of buffers in each group */
#define PBM_TRACK_BLOCKGROUP_BUFFERS (PBM_USE_PQ || (PBM_EVICT_MODE == PBM_EVICT_MODE_SAMPLING))

/* Whether to evict multiple at once with sampling-based PBM */
#define PBM_SAMPLING_EVICT_MULTI

/* Track average eviction time and periodically log it. */
//#define PBM_TRACK_EVICTION_TIME

#endif //POSTGRESQL_PBM_CONFIG_H
