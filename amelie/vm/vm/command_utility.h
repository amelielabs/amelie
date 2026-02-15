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

// system
void ccheckpoint(Vm*, Op*);

// users
void cuser_create_token(Vm*, Op*);
void cuser_create(Vm*, Op*);
void cuser_drop(Vm*, Op*);
void cuser_alter(Vm*, Op*);

// replica
void creplica_create(Vm*, Op*);
void creplica_drop(Vm*, Op*);

// replication
void crepl_start(Vm*, Op*);
void crepl_stop(Vm*, Op*);
void crepl_subscribe(Vm*, Op*);
void crepl_unsubscribe(Vm*, Op*);

// ddl
void cddl(Vm*, Op*);
void cddl_create_index(Vm*, Op*);

// locking
void clock_rel(Vm*, Op*);
void cunlock_rel(Vm*, Op*);
