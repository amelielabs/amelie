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

typedef struct SaveWorker SaveWorker;
typedef struct Save       Save;

struct SaveWorker
{
	int    list_count;
	List   list;
	Notify notify;
	Event  on_complete;
	pid_t  pid;
};

struct Save
{
	uint64_t    lsn;
	SaveWorker* workers;
	int         workers_count;
	Catalog*    catalog;
};

void save_init(Save*, Catalog*);
void save_free(Save*);
void save_begin(Save*, uint64_t, int);
void save_run(Save*);
void save_wait(Save*);
