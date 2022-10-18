/*
 * Some minimal declarations to reduce need to recompile on unrelated changes...
 */

#ifndef POSTGRESQL_PBM_MINIMAL_H
#define POSTGRESQL_PBM_MINIMAL_H

typedef size_t ScanId;

struct PBM_ScanHashEntry;


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
	// TODO probably need an index of the next position in the block_groups thing for report
};


#endif //POSTGRESQL_PBM_MINIMAL_H
