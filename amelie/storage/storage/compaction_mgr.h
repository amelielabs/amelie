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

typedef struct CompactionMgr CompactionMgr;

struct CompactionMgr
{
	int          workers_count;
	Compaction** workers;
};

static inline void
compaction_mgr_init(CompactionMgr* self)
{
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
compaction_mgr_start(CompactionMgr* self, Storage* storage, int count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = am_malloc(sizeof(Compaction) * count);
	int i = 0;
	for (; i < count; i++)
		self->workers[i] = compaction_allocate(storage);
	for (i = 0; i < count; i++)
		compaction_start(self->workers[i]);
}

static inline void
compaction_mgr_stop(CompactionMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
   	{
		compaction_stop(self->workers[i]);
		compaction_free(self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}
