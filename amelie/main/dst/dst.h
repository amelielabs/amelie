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

typedef struct Dst Dst;

struct Dst
{
	DstUser* users;
	int      users_count;
	int64_t  step;
	Opt      opt_dir;
	Opt      opt_seed;
	Opt      opt_users;
	Opt      opt_steps;
	Opt      opt_keys;
	Opt      opt_rels;
	Opt      opt_sync;
	Opt      opt_restart;
	Opt      opt_checkpoint;
	Opt      opt_ddl;
	Opt      opt_bp;
	Opts     opts;
	Runtime  runtime;
};

void dst_init(Dst*);
void dst_free(Dst*);
void dst_execute(Dst*, Client*, char*, ...);
bool dst_execute_log(DstUser*);
void dst_run(Dst*);
