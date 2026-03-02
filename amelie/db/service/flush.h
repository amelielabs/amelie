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

typedef struct Flush       Flush;
typedef struct FlushBranch FlushBranch;

struct FlushBranch
{
	ServiceLock lock;
	Object*     parent;
	Branch*     branch;
};

struct Flush
{
	ServiceLock  lock;
	// origin
	Part*        origin;
	Id           origin_id;
	uint64_t     origin_lsn;
	Index*       origin_indexes;
	Buf          origin_heap_index;
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

void flush_init(Flush*, Service*);
void flush_free(Flush*);
void flush_reset(Flush*);
bool flush_run(Flush*, Table*, uint64_t);
