/*-------------------------------------------------------------------------
 * Predictive Buffer Manager
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRESQL_PBM_H
#define POSTGRESQL_PBM_H

#include "storage/pbm/pbm_minimal.h"
#include "storage/pbm/pbm_config.h"

#include "storage/block.h"


/* ===== GLOBAL CONFIGURATION VARIABLES ===== */
#define PBM_EVICT_MAX_SAMPLES 256
extern int pbm_evict_num_samples;
extern int pbm_evict_num_victims;
extern double pbm_bg_naest_max_age_s;  /* not actually used except by GUC */
extern unsigned long pbm_bg_naest_max_age_ns;
extern bool pbm_evict_whole_group;
extern bool pbm_evict_use_freq;
extern bool pbm_evict_use_idx_scan;
extern int pbm_idx_scan_num_counts;


/* ===== FORWARD DECLARATIONS (sorted) ===== */

struct BitmapHeapScanState;
struct BlockGroupData;
struct BufferDesc;
struct HeapScanDescData;
struct IndexScanDescData;
struct IndexScanState;
struct ParallelBlockTableScanDescData;
struct ParallelContext;
struct ParallelIndexScanDescData;


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
extern void PBM_UnregisterBitmapScan(struct BitmapHeapScanState * scan, char* msg/*### (to be removed)*/);
extern void internal_PBM_ReportBitmapScanPosition(struct BitmapHeapScanState *scan, BlockNumber pos);


/* ===== INDEX SCAN METHODS ===== */

extern void PBM_RegisterIndexScan(struct IndexScanState * scan, struct ParallelIndexScanDescData * pscan);
extern void PBM_UnregisterIndexScan(struct IndexScanState * scan);
extern void PBM_ReportIndexScanPosition(struct IndexScanDescData * scan);
extern void PBM_ReportIndexScanRescan(struct IndexScanState * scan);


/* ===== BUFFER TRACKING ===== */

extern void PbmNewBuffer(struct BufferDesc * buf);
extern void PbmOnEvictBuffer(struct BufferDesc * buf);

#if PBM_TRACK_STATS
extern void PbmOnAccessBuffer(const struct BufferDesc * buf);
#endif /* PBM_TRACK_STATS */

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
#elif PBM_EVICT_MODE == PBM_EVICT_MODE_SAMPLING
extern struct BufferDesc* PBM_EvictPage(uint32 * buf_state);
#endif


/* ===== DEBUGGING (to be removed) ===== */

extern void PBM_DEBUG_sanity_check_buffers(void);
extern void PBM_DEBUG_print_pbm_state(void);

#if defined(PBM_TRACK_EVICTION_TIME)
extern uint64 PBM_DEBUG_CUR_TIME_ns();
extern void PBM_DEBUG_eviction_timing(uint64 start);
#endif /* PBM_TRACK_EVICTION_TIME */

#endif //POSTGRESQL_PBM_H
