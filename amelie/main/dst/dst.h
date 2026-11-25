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
	uint64_t dbs_seq;
	List     dbs;
	int      dbs_count;
	int64_t  step;
	DstStat  stats;
	Str      opt_dir;
	int64_t  opt_seed;
	int64_t  opt_dbs;
	int64_t  opt_steps;
	int64_t  opt_keys;
	int64_t  opt_payload;
	int64_t  opt_rels;
	int64_t  opt_sync;
	int64_t  opt_restart;
	int64_t  opt_checkpoint;
	int64_t  opt_backup;
	int64_t  opt_ddl;
	int64_t  opt_ddl_db;
	int64_t  opt_bp;
	int64_t  opt_track_key;
	Opts     opts;
	Buf      payload;
	Runtime  runtime;
};

void dst_init(Dst*);
void dst_free(Dst*);
void dst_execute(Dst*, Client*, char*, ...);
bool dst_execute_log(DstDb*);
void dst_run(Dst*);

static inline DstDb*
dst_db(Dst* self, int order)
{
	auto pos = 0;
	list_foreach(&self->dbs)
	{
		auto db = list_at(DstDb, link);
		if (pos == order)
			return db;
		pos++;
	}
	return NULL;
}
