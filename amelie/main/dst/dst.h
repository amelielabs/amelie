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
	uint64_t users_seq;
	List     users;
	int      users_count;
	int64_t  step;
	DstStat  stats;
	Opt      opt_dir;
	Opt      opt_seed;
	Opt      opt_users;
	Opt      opt_steps;
	Opt      opt_keys;
	Opt      opt_payload;
	Opt      opt_parts;
	Opt      opt_rels;
	Opt      opt_sync;
	Opt      opt_restart;
	Opt      opt_checkpoint;
	Opt      opt_backup;
	Opt      opt_ddl;
	Opt      opt_ddl_user;
	Opt      opt_bp;
	Opts     opts;
	Buf      payload;
	Runtime  runtime;
};

void dst_init(Dst*);
void dst_free(Dst*);
void dst_execute(Dst*, Client*, char*, ...);
bool dst_execute_log(DstUser*);
void dst_run(Dst*);

static inline DstUser*
dst_user(Dst* self, int order)
{
	auto pos = 0;
	list_foreach(&self->users)
	{
		auto user = list_at(DstUser, link);
		if (pos == order)
			return user;
		pos++;
	}
	return NULL;
}
