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
	ServiceLock  lock;
	Part*        origin;
	uint64_t     origin_lsn;
	Id           id_origin;
	Id           id_ram;
	File         file_ram;
	ServiceFile* service_file;
	Service*     service;
	Table*       table;
};

void refresh_init(Refresh*, Service*);
void refresh_free(Refresh*);
void refresh_reset(Refresh*);
bool refresh_run(Refresh*, Table*, uint64_t, Str*);
