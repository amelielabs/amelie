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

typedef struct WriteList WriteList;

struct WriteList
{
	List list;
	int  list_count;
};

static inline void
write_list_init(WriteList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
write_list_reset(WriteList* self)
{
	write_list_init(self);
}

static inline void
write_list_add(WriteList* self, Write* write)
{
	list_append(&self->list, &write->link);
	self->list_count++;
}
