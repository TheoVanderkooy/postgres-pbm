/*-------------------------------------------------------------------------
 * Predictive Buffer Manager
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRESQL_PBM_H
#define POSTGRESQL_PBM_H

#include "storage/pbm/pbm_minimal.h"

#include "storage/block.h"


/* ===== CONFIGURATION ===== */

// (move this to pg_config_manual.h later, for now it is here because changing it forces *everything* to recompile)
/*
 * What eviction implementation to use:
 *  0: existing clock algorithm w/ "strategies" (default if USE_PBM is disabled)
 *  1: first implementation with only 1 block at a time (might not work anymore!)
 *  2: method that puts still-valid blocks on the free list and lets the normal
 *     mechanism try multiple times to get from the free list
 */
#define PBM_EVICT_MODE_CLOCK 0
#define PBM_EVICT_MODE_SINGLE 1
#define PBM_EVICT_MODE_MULTI 2
#ifdef USE_PBM
#define PBM_EVICT_MODE PBM_EVICT_MODE_MULTI
#else
#define PBM_EVICT_MODE PBM_EVICT_MODE_CLOCK
#endif


/* ===== FORWARD DECLARATIONS (sorted) ===== */

struct BitmapHeapScanState;
struct BlockGroupData;
struct BufferDesc;
struct HeapScanDescData;


/* ===== TYPES DEFINITIONS ===== */

/* Scan statistics that don't need to be shared */
struct PBM_LocalSeqScanStats{
	unsigned long last_report_time;
// ### is this already in the scan descriptor?? use that instead?
	BlockNumber	last_pos;	/* This is in terms of # of *blocks*, not block groups */
};


/* ===== INITIALIZATION ===== */

extern void InitPBM(void);
extern Size PbmShmemSize(void);


/* ===== SEQUENTIAL SCAN METHODS ===== */

extern void PBM_RegisterSeqScan(struct HeapScanDescData * scan);
extern void PBM_UnregisterSeqScan(struct HeapScanDescData * scan);
extern void PBM_ReportSeqScanPosition(struct HeapScanDescData *scan, BlockNumber pos);


/* ===== BITMAP SCAN METHODS ===== */

extern void PBM_RegisterBitmapScan(struct BitmapHeapScanState * scan);
extern void PBM_UnregisterBitmapScan(struct BitmapHeapScanState * scan, char* msg/*(to be removed)*/);
extern void PBM_ReportBitmapScanPosition(struct BitmapHeapScanState * scan /*TODO other args?*/);


/* ===== BUFFER TRACKING ===== */

extern void PbmNewBuffer(struct BufferDesc * buf);
extern void PbmOnEvictBuffer(struct BufferDesc * buf);


/* ===== EVICTION METHODS ===== */
#if PBM_EVICT_MODE == PBM_EVICT_MODE_SINGLE
extern struct BufferDesc* PBM_EvictPage(uint32 * buf_state);
#elif PBM_EVICT_MODE == PBM_EVICT_MODE_MULTI
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
