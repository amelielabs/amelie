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

typedef struct Split Split;

struct Split
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

void split_init(Split*, Service*);
void split_free(Split*);
void split_reset(Split*);
bool split_run(Split*, Table*, uint64_t);
