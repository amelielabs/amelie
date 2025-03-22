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

typedef struct Commit Commit;

struct Commit
{
	int       list_count;
	List      list;
	WriteList write_list;
};

static inline void
commit_init(Commit* self)
{
	self->list_count = 0;
	list_init(&self->list);
	write_list_init(&self->write_list);
}

static inline void
commit_free(Commit* self)
{
	(void)self;
}

static inline void
commit_reset(Commit* self)
{
	self->list_count = 0;
	list_init(&self->list);
	write_list_reset(&self->write_list);
}

static inline void
commit_add(Commit* self, Dtr* tr)
{
	// collect unique list of all partitions
		// todo:
	list_append(&self->list, &tr->link_commit);
	self->list_count++;
}
