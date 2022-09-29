/*-------------------------------------------------------------------------
 * Predictive Buffer Manager
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRESQL_PBM_H
#define POSTGRESQL_PBM_H

#include "storage/block.h"


/// CONFIGURATION
// (move this to pg_config_manual.h later, for now it is here because changing it forces *everything* to recompile)
/*
 * What eviction implementation to use:
 *  0: existing clock algorithm w/ "strategies" (default if USE_PBM is disabled)
 *  1: first implementation with only 1 block at a time
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


//#define PBM_TRACE_FREE_LIST


/// Forward declarations
struct HeapScanDescData;
struct BufferDesc;
struct BlockGroupData;
struct PBM_ScanData;


/// Type declarations used elsewhere
typedef size_t ScanId;

// Store scan statistics locally when they don't need to be read by other processes
struct PBM_LocalScanStats{
	long last_report_time;
	BlockNumber	last_pos;
};


/// Initialization
extern void InitPBM(void);
extern Size PbmShmemSize(void);

/// Public API called by *sequential* scan operators
extern void RegisterSeqScan(struct HeapScanDescData * scan);
extern void UnregisterSeqScan(struct HeapScanDescData * scan);
extern void ReportSeqScanPosition(struct HeapScanDescData *scan, BlockNumber pos);

/// Forward certain operations from the normal buffer manager to PBM.
extern void PbmNewBuffer(struct BufferDesc * buf);
//extern void PbmBufferNotPinned(struct BufferDesc * buf);
extern void PbmOnEvictBuffer(struct BufferDesc * buf);

/// Eviction-related methods & data structures
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

/// Debugging sanity checks
extern void PBM_sanity_check_buffers(void);
extern void PBM_print_pbm_state(void);

/*
 * TODO: Register*Scan for:
 *  - BRIN index scans
 *  - Bitmap scans
 *  - BTree index scans (& others?) (exclude hash index?)
 *  ^ These will need to specify ranges maybe
 *  ^ note for non-heap scans: consider passing "scan key"
 */

#endif //POSTGRESQL_PBM_H
