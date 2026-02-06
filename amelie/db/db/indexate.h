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

typedef struct Indexate Indexate;

struct Indexate
{
	PartLock lock;
	Part*    origin;
	Index*   index;
	Table*   table;
	Db*      db;
};

void indexate_init(Indexate*, Db*);
void indexate_reset(Indexate*);
void indexate_run(Indexate*, Uuid*, uint64_t, IndexConfig*);
