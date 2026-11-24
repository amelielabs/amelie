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

typedef struct Writes Writes;

struct Writes
{
	List     list;
	int      list_count;
	uint64_t lsn;
};

static inline void
writes_init(Writes* self)
{
	self->list_count = 0;
	self->lsn        = 0;
	list_init(&self->list);
}

static inline void
writes_reset(Writes* self)
{
	writes_init(self);
}

static inline void
writes_add(Writes* self, Write* write)
{
	list_append(&self->list, &write->msg.link);
	self->list_count++;
}
