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

struct RefreshObject
{
	ServiceLock  lock;
	Object*      origin;
	Id           origin_id;
	Object*      object;

	Writer*      writer;
	Table*       table;
	ServiceFile* service_file;
	Service*     service;
};

void refresh_object_init(RefreshObject*, Service*);
void refresh_object_free(RefreshObject*);
void refresh_object_reset(RefreshObject*);
bool refresh_object_run(RefreshObject*, Table*, uint64_t);
