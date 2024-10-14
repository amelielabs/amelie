#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Commit Commit;

struct Commit
{
	PipeSet  set;
	int      list_count;
	List     list;
	WalBatch wal_batch;
};

static inline void
commit_init(Commit* self)
{
	self->list_count = 0;
	list_init(&self->list);
	pipe_set_init(&self->set);
	wal_batch_init(&self->wal_batch);
}

static inline void
commit_free(Commit* self)
{
	pipe_set_free(&self->set);
	wal_batch_free(&self->wal_batch);
}

static inline void
commit_reset(Commit* self)
{
	self->list_count = 0;
	list_init(&self->list);
	pipe_set_reset(&self->set, NULL);
	wal_batch_reset(&self->wal_batch);
}

static inline void
commit_prepare(Commit* self, int set_size)
{
	pipe_set_create(&self->set, set_size);
}

static inline void
commit_add(Commit* self, Dtr* tr)
{
	// collect a list of last completed transactions per route
	pipe_set_resolve(&tr->set, &self->set);

	list_append(&self->list, &tr->link_commit);
	self->list_count++;
}
