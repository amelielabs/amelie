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

typedef struct Evict       Evict;
typedef struct EvictBranch EvictBranch;

struct EvictBranch
{
	ServiceLock lock;
	Object*     parent;
	bool        parent_last;
	Branch*     branch;
};

struct Evict
{
	ServiceLock  lock;
	// origin
	Part*        origin;
	Id           origin_id;
	uint64_t     origin_lsn;
	Index*       origin_indexes;
	Collection   origin_col;
	// updated partition
	Id           part_id;
	File         part_file;
	// branches
	Buf          branches;
	int          branches_count;
	Writer*      writer;
	Table*       table;
	ServiceFile* service_file;
	Service*     service;
};

void evict_init(Evict*, Service*);
void evict_free(Evict*);
void evict_reset(Evict*);
bool evict_run(Evict*, Table*, uint64_t);
