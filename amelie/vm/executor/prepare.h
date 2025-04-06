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
	uint64_t  id_min;
	uint64_t  id_max;
	int       list_count;
	List      list;
	WriteList write;
};

static inline void
prepare_init(Prepare* self)
{
	self->id_min      = 0;
	self->id_max      = 0;
	self->list_count  = 0;
	list_init(&self->list);
	write_list_init(&self->write);
}

static inline void
prepare_reset(Prepare* self)
{
	self->id_min     = 0;
	self->id_max     = 0;
	self->list_count = 0;
	list_init(&self->list);
	write_list_reset(&self->write);
}

hot static inline void
prepare_add(Prepare* self, Dtr* dtr)
{
	// add transaction to the list
	list_append(&self->list, &dtr->link_prepare);
	self->list_count++;

	// track min/max transaction id
	if (dtr->id < self->id_min)
		self->id_min = dtr->id;
	if (dtr->id > self->id_max)
		self->id_max = dtr->id;
}
