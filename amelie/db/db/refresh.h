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

typedef struct Refresh Refresh;

struct Refresh
{
	OpsLock  lock;
	Part*    origin;
	uint64_t origin_lsn;
	Id       id_origin;
	Id       id_ram;
	Id       id_service;
	File     file_ram;
	File     file_service;
	Service* service;
	Table*   table;
	Db*      db;
};

void refresh_init(Refresh*, Db*);
void refresh_free(Refresh*);
void refresh_reset(Refresh*);
void refresh_run(Refresh*, Uuid*, uint64_t, Str*);
