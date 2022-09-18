/*-------------------------------------------------------------------------
 * Predictive Buffer Manager
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRESQL_PBM_H
#define POSTGRESQL_PBM_H

#include "storage/block.h"


/// Forward declarations
struct HeapScanDescData;
struct BufferDesc;

/// Type declarations used elsewhere
typedef size_t ScanId;

/// Initialization
extern void InitPBM(void);
extern Size PbmShmemSize(void);

/// Public API called by *sequential* scan operators
extern void RegisterSeqScan(struct HeapScanDescData * scan);
extern void UnregisterSeqScan(struct HeapScanDescData * scan);
extern void ReportSeqScanPosition(ScanId id, BlockNumber pos);

/// Forward certain operations from the normal buffer manager to PBM.
extern void PbmNewBuffer(struct BufferDesc * buf);
//extern void PbmBufferNotPinned(struct BufferDesc * buf);
extern void PbmOnEvictBuffer(struct BufferDesc * buf);

/// Eviction-related methods & data structures
#if PBM_EVICT_MODE == 1
extern struct BufferDesc* PBM_EvictPage(void);
#elif PBM_EVICT_MODE == 2
typedef struct PBM_EvictState {
	int next_idx;
} PBM_EvictState;

extern void PBM_InitEvictState(PBM_EvictState * state);
extern void PBM_EvictPages(PBM_EvictState * state);
static inline bool PBM_EvictingFailed(PBM_EvictState * state) { return state->next_idx >= 0; }
#endif

/*
 * TODO: Register*Scan for:
 *  - BRIN index scans
 *  - Bitmap scans
 *  - BTree index scans (& others?) (exclude hash index?)
 *  ^ These will need to specify ranges maybe
 *  ^ note for non-heap scans: consider passing "scan key"
 */

#endif //POSTGRESQL_PBM_H
