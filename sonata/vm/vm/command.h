#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

Op*  ccursor_open(Vm*, Op*);
Op*  ccursor_open_expr(Vm*, Op*);
Op*  ccursor_open_cte(Vm*, Op*);
void ccursor_prepare(Vm*, Op*);
void ccursor_close(Vm*, Op*);
Op*  ccursor_next(Vm*, Op*);
void ccursor_read(Vm*, Op*);
void ccursor_idx(Vm*, Op*);

void ccall(Vm*, Op*);

void cinsert(Vm*, Op*);
void cupdate(Vm*, Op*);
void cdelete(Vm*, Op*);
Op*  cupsert(Vm*, Op*);

void cmerge(Vm*, Op*);
void cmerge_recv(Vm*, Op*);
void cgroup_merge_recv(Vm*, Op*);

void csend(Vm*, Op*);
void csend_first(Vm*, Op*);
void csend_all(Vm*, Op*);
void crecv(Vm*, Op*);
void crecv_to(Vm*, Op*);
