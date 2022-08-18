/*-------------------------------------------------------------------------
 * TODO theo description...
 *-------------------------------------------------------------------------
 */
#ifndef POSTGRESQL_PBM_H
#define POSTGRESQL_PBM_H

// Forward declarations
struct HeapScanDescData;

// Type declarations used elsewhere
typedef size_t ScanId;


// Initialization
extern void InitPBM(void);
extern Size PbmShmemSize(void);
// TODO theo -- may need local initialization if we can track some things only for the local process (e.g. scans, only need some info tracked to be shared)

// Public API called by scan operators
extern void RegisterSeqScan(struct HeapScanDescData * scan);
//extern void RegisterSeqScan(HeapScanDesc scan/*TODO theo args -- table, [columns-n/a], [ranges?]*/);
extern void UnregisterScan(ScanId id);
extern void ReportScanPosition(/*TODO theo args -- ??? scan operator/id should be good enough*/);

/*
 * TODO: Register*Scan for:
 *  - BRIN index scans
 *  - Bitmap scans
 *  - BTree index scans (& others?) (exclude hash index?)
 * ^ These will need to specify ranges maybe
 *
 * May need separate "unregister" and "report position" methods too?
 *
 * TODO: other:
 *  - do we care
 */

/*
 *
 */


#endif //POSTGRESQL_PBM_H
