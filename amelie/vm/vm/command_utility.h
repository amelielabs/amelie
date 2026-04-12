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

// storage
void ccheckpoint(Vm*, Op*);

// system
void ccreate_secret(Vm*, Op*);
void ccreate_token(Vm*, Op*);

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
