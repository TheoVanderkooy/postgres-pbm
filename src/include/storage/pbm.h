/*-------------------------------------------------------------------------
 * Predictive Buffer Manager
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRESQL_PBM_H
#define POSTGRESQL_PBM_H

#include "storage/pbm/pbm_minimal.h"

#include "storage/block.h"


/* ===== CONFIGURATION ===== */

// (move this to pg_config_manual.h later, for now it is here because changing pg_config_manual.h forces *everything* to recompile)
/*
 * What eviction implementation to use:
 *  0: existing clock algorithm w/ "strategies" (default if USE_PBM is disabled)
 *  1: first implementation with only 1 block at a time (might not work anymore!)
 *  2: method that puts still-valid blocks on the free list and lets the normal
 *     mechanism try multiple times to get from the free list
 */
#define PBM_EVICT_MODE_CLOCK 0
#define PBM_EVICT_MODE_PQ_SINGLE 1
#define PBM_EVICT_MODE_PQ_MULTI 2
#define PBM_EVICT_MODE_SAMPLING 3
#ifdef USE_PBM
#define PBM_EVICT_MODE PBM_EVICT_MODE_PQ_MULTI
//#define PBM_EVICT_MODE PBM_EVICT_MODE_SAMPLING
#else
#define PBM_EVICT_MODE PBM_EVICT_MODE_CLOCK
#endif

#if (PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_SINGLE) || (PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_MULTI)
#define PBM_USE_PQ true
#else
#define PBM_USE_PQ false
#endif


/* ===== GLOBAL CONFIGURATION VARIABLES ===== */
int pbm_evict_num_samples;


/* ===== FORWARD DECLARATIONS (sorted) ===== */

struct BitmapHeapScanState;
struct BlockGroupData;
struct BufferDesc;
struct HeapScanDescData;
struct ParallelBlockTableScanDescData;
struct ParallelContext;


/* ===== TYPES DEFINITIONS ===== */

/* Scan statistics that don't need to be shared */
struct PBM_LocalSeqScanStats {
	unsigned long last_report_time;
// ### is this already in the scan descriptor?? use that instead?
	BlockNumber	last_pos;	/* This is in terms of # of *blocks*, not block groups */

	pbm_bg_iterator bg_it;
};


/* ===== INITIALIZATION ===== */

extern void InitPBM(void);
extern Size PbmShmemSize(void);


/* ===== SEQUENTIAL SCAN METHODS ===== */

extern void PBM_RegisterSeqScan(struct HeapScanDescData *scan, struct ParallelContext *pctx);
extern void PBM_UnregisterSeqScan(struct HeapScanDescData * scan);
extern void internal_PBM_ReportSeqScanPosition(struct HeapScanDescData * scan, BlockNumber pos);

extern void PBM_InitParallelSeqScan(struct HeapScanDescData * scan, BlockNumber pos);
extern void internal_PBM_ParallelWorker_ReportSeqScanPosition(struct HeapScanDescData *scan,
		struct ParallelBlockTableScanDescData * pbscan, BlockNumber cur_page, BlockNumber new_page);


/* ===== BITMAP SCAN METHODS ===== */

extern void PBM_RegisterBitmapScan(struct BitmapHeapScanState * scan);
extern void PBM_UnregisterBitmapScan(struct BitmapHeapScanState * scan, char* msg/*(to be removed)*/);
extern void internal_PBM_ReportBitmapScanPosition(struct BitmapHeapScanState *scan, BlockNumber pos);


/* ===== BUFFER TRACKING ===== */

#if PBM_USE_PQ
extern void PbmNewBuffer(struct BufferDesc * buf);
extern void PbmOnEvictBuffer(struct BufferDesc * buf);
#endif /* PBM_USE_PQ */

/* ===== EVICTION METHODS ===== */
#if PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_SINGLE
extern struct BufferDesc* PBM_EvictPage(uint32 * buf_state);
#elif PBM_EVICT_MODE == PBM_EVICT_MODE_PQ_MULTI
typedef struct PBM_EvictState {
	int next_bucket_idx;
} PBM_EvictState;

extern void PBM_InitEvictState(PBM_EvictState * state);
extern void PBM_EvictPages(PBM_EvictState * state);
static inline bool PBM_EvictingFailed(PBM_EvictState * state) {
	return state->next_bucket_idx < -1;
}
#endif


/* ===== DEBUGGING (to be removed) ===== */

extern void PBM_DEBUG_sanity_check_buffers(void);
extern void PBM_DEBUG_print_pbm_state(void);


#endif //POSTGRESQL_PBM_H
