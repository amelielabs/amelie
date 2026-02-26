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

typedef struct Flush Flush;

struct Flush
{
	ServiceLock  lock;
	Part*        origin;
	uint64_t     origin_lsn;
	Id           id_origin;
	Id           id_part;
	Id           id_branch;
	File         file_part;
	File         file_branch;
	Index*       indexes;
	Buf          heap_index;
	Object*      object;
	Writer*      writer;
	Table*       table;
	ServiceFile* service_file;
	Service*     service;
};

void flush_init(Flush*, Service*);
void flush_free(Flush*);
void flush_reset(Flush*);
bool flush_run(Flush*, Table*, uint64_t);
