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

typedef struct Merge Merge;

struct Merge
{
	ServiceLock  lock;
	Object*      origin;
	Buf          objects;
	int          objects_count;
	Writer*      writer;
	Table*       table;
	Tier*        tier;
	ServiceFile* service_file;
	Service*     service;
};

void merge_init(Merge*, Service*);
void merge_free(Merge*);
void merge_reset(Merge*);
bool merge_run(Merge*, Table*, uint64_t);
