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

enum
{
	DST_STEP_OP,
	DST_STEP_ERROR,
	DST_STEP_VALIDATE,
	DST_STEP_RESTART
};

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
	Opt      opt_sync;
	Opts     opts;
	Runtime  runtime;
};

void dst_init(Dst*);
void dst_free(Dst*);
void dst_execute(Dst*, Client*, bool, char*, ...);
void dst_execute_log(DstUser*);
void dst_run(Dst*);
