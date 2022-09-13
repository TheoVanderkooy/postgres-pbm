/// Predictive buffer manager functions called in the background for maintenance.
/// Separate header for these since it is used in very different locations than other PBM methods.

#ifndef POSTGRESQL_PBM_BACKGROUND_H
#define POSTGRESQL_PBM_BACKGROUND_H

extern void PBM_TryRefreshRequestedBuckets(void);

#endif //POSTGRESQL_PBM_BACKGROUND_H
