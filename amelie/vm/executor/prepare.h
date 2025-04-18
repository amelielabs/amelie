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

typedef struct Prepare Prepare;

struct Prepare
{
	PipeSet   set;
	int       list_count;
	List      list;
	WriteList write_list;
};

static inline void
prepare_init(Prepare* self)
{
	self->list_count = 0;
	list_init(&self->list);
	pipe_set_init(&self->set);
	write_list_init(&self->write_list);
}

static inline void
prepare_free(Prepare* self)
{
	pipe_set_free(&self->set);
}

static inline void
prepare_reset(Prepare* self)
{
	self->list_count = 0;
	list_init(&self->list);
	pipe_set_reset(&self->set, NULL);
	write_list_reset(&self->write_list);
}

static inline void
prepare_prepare(Prepare* self, int set_size)
{
	pipe_set_create(&self->set, set_size);
}

static inline void
prepare_add(Prepare* self, Dtr* tr)
{
	// collect a list of last completed transactions per route
	pipe_set_resolve(&tr->set, &self->set);

	list_append(&self->list, &tr->link_prepare);
	self->list_count++;
}
