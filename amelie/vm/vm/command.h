#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

void csend_shard(Vm*, Op*);
void csend_lookup(Vm*, Op*);
void csend_lookup_by(Vm*, Op*);
void csend_all(Vm*, Op*);
void cclose(Vm*, Op*);

void cunion_set(Vm*, Op*);
void cunion_recv(Vm*, Op*);
void cvar_set(Vm*, Op*);

Op*  ctable_open(Vm*, Op*, bool, bool);
Op*  ctable_open_heap(Vm*, Op*);
void ctable_prepare(Vm*, Op*);

void cinsert(Vm*, Op*);
Op*  cupsert(Vm*, Op*);
void cdelete(Vm*, Op*);
void cupdate(Vm*, Op*);
void cupdate_store(Vm*, Op*);
