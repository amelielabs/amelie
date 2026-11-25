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
	uint64_t opt_seed;
	uint32_t opt_dbs;
	uint32_t opt_steps;
	uint32_t opt_keys;
	uint32_t opt_payload;
	uint32_t opt_rels;
	uint32_t opt_sync;
	uint32_t opt_restart;
	uint32_t opt_checkpoint;
	uint32_t opt_backup;
	uint32_t opt_ddl;
	uint32_t opt_ddl_db;
	uint32_t opt_bp;
	uint32_t opt_track_key;
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
