#ifndef OGGIT_MERGE_BARRIER_H
#define OGGIT_MERGE_BARRIER_H

#include "executor/exec/execdesc.h"

void pg_init_oggit_merge_barrier(void);
void oggit_merge_barrier_check_executor(QueryDesc *query_desc);

#endif