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
	OpsLock  lock;
	Part*    origin;
	uint64_t origin_lsn;
	Id       id_origin;
	Id       id_ram;
	Id       id_pending;
	Id       id_service;
	File     file_ram;
	File     file_pending;
	File     file_service;
	Index*   indexes;
	Buf      heap_index;
	Object*  object;
	Service* service;
	Writer*  writer;
	Table*   table;
	Db*      db;
};

void flush_init(Flush*, Db*);
void flush_free(Flush*);
void flush_reset(Flush*);
void flush_run(Flush*, Uuid*, uint64_t);
