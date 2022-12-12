/*
 * Some inline functions that can't be in the main header due to include order
 * dependencies...
 */

#ifndef POSTGRESQL_PBM_INLINE_H
#define POSTGRESQL_PBM_INLINE_H

#include <stddef.h>

#include "storage/block.h"
#include "access/heapam.h"
#include "nodes/execnodes.h"

#include "storage/pbm/pbm_internal.h"

static inline void PBM_ReportSeqScanPosition(struct HeapScanDescData * scan, BlockNumber pos) {
	const BlockNumber prevGroupPos	= BLOCK_GROUP(scan->pbmLocalScanStats.last_pos);
	const BlockNumber curGroupPos	= BLOCK_GROUP(pos);

	/* Nothing to do if the scan is not registered */
	if (NULL == scan->pbmSharedScanData) {
		return;
	}

	/* Only update stats periodically: when we reach a new block group. */
	if (prevGroupPos == curGroupPos) {
		return;
	}

	/* Call the actual method only after the common-case immediate return. */
	internal_PBM_ReportSeqScanPosition(scan, pos);
}


static inline void PBM_ParallelWorker_ReportSeqScanPosition(struct HeapScanDescData *scan, struct ParallelBlockTableScanDescData * pbscan,
															BlockNumber cur_page, BlockNumber new_page) {
	ParallelBlockTableScanWorkerData * pbscanwork = scan->rs_parallelworkerdata;
	BlockNumber last_reported;

	/* Parallel scan is not registered, ignore it */
	if (NULL == pbscan->pbmSharedScanData) {
		return;
	}

	/* Sanity checks */
	Assert(pbscanwork != NULL);

	/* update local progress if we are on a new block group, or a new chunk
 	 * not contiguous with the previous */
	last_reported = pbscanwork->pbm_last_reported_pos;
	if (BLOCK_GROUP(cur_page) != BLOCK_GROUP(last_reported)
		|| new_page != cur_page+1
		|| new_page == InvalidBlockNumber)
	{
		/* Do the real work. */
		internal_PBM_ParallelWorker_ReportSeqScanPosition(scan, pbscan, cur_page, new_page);
	}
}


static inline void PBM_ReportBitmapScanPosition(struct BitmapHeapScanState *scan, BlockNumber pos) {
	const BlockNumber bg = BLOCK_GROUP(pos);

	/* Nothing to do if the scan is not registered. I *think* this will happen
 	 * with the non-leader parallel workers for a parallel bitmap scan */
	if (NULL == scan->pbmSharedScanData) {
		return;
	}

	/* Nothing to do if this isn't a new block group. */
	if (bg == BLOCK_GROUP(scan->pbmLocalScanData.last_pos)) {
		return;
	}

	/* Do the real work. */
	internal_PBM_ReportBitmapScanPosition(scan, pos);
}

#endif //POSTGRESQL_PBM_INLINE_H
