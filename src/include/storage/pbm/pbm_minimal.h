/*
 * Some minimal declarations to reduce need to recompile on unrelated changes...
 */

#ifndef POSTGRESQL_PBM_MINIMAL_H
#define POSTGRESQL_PBM_MINIMAL_H

#include "storage/relfilenode.h"

typedef size_t ScanId;

struct PBM_ScanHashEntry;


/* Key in Block Group Data Map */
typedef struct BlockGroupHashKey {
	RelFileNode	rnode;		// physical relation
	ForkNumber	forkNum;	// fork in the relation
	uint32		seg;		// "block group segment" in the hash table
} BlockGroupHashKey;

/* Track location in block group map to avoid hash lookups */
typedef struct pbm_bg_iterator {
	BlockGroupHashKey bgkey;
	struct BlockGroupHashEntry * entry;
} pbm_bg_iterator;

/*
 * State needed for tracking bitmap scans. Track a vector of (Block group, # blocks in group)
 * pairs to calculate "blocks behind" for each group + determine how much progress we've made.
 */
struct bg_ct_pair {
	uint32 block_group;
	uint16 blk_cnt;
};

typedef struct bgcnt_vec {
	int len;
	int cap;
	struct bg_ct_pair * items;
} bgcnt_vec;

struct PBM_LocalBitmapScanState {
	unsigned long last_report_time;
	uint32 last_pos;   /* This is in terms of *blocks*, not block groups */
	bgcnt_vec block_groups;	/* Record interested block groups wtih # of blocks in each */
	int vec_idx; /* Track current position in `block_groups` as we report progress */

	pbm_bg_iterator bg_it;
};


#endif //POSTGRESQL_PBM_MINIMAL_H
