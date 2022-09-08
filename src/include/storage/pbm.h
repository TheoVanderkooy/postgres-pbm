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

extern struct BufferDesc* EvictPages(void);

/*
 * TODO: Register*Scan for:
 *  - BRIN index scans
 *  - Bitmap scans
 *  - BTree index scans (& others?) (exclude hash index?)
 *  ^ These will need to specify ranges maybe
 *  ^ note for non-heap scans: consider passing "scan key"
 */


#endif //POSTGRESQL_PBM_H
