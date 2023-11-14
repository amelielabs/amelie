#pragma once

//
// monotone
//
// SQL OLTP database
//

Op*  ccursor_open(Vm*, Op*);
Op*  ccursor_open_expr(Vm*, Op*);
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
void csend_set(Vm*, Op*);
