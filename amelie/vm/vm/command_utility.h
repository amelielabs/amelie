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
void csystem_set_secret(Vm*, Op*);
void csystem_set_cdc(Vm*, Op*);
void ccreate_token(Vm*, Op*);

// db
void ccheckpoint(Vm*, Op*);
void cbackup(Vm*, Op*);

// replica
void creplica_create(Vm*, Op*);
void creplica_drop(Vm*, Op*);

// replication
void crepl_start(Vm*, Op*);
void crepl_stop(Vm*, Op*);
void crepl_follow(Vm*, Op*);
void crepl_unfollow(Vm*, Op*);

// ddl
void cddl(Vm*, Op*);
void cddl_create_index(Vm*, Op*);

// locking
void clock_rel(Vm*, Op*);
void cunlock_rel(Vm*, Op*);
