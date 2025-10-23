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

typedef struct SetList SetList;

struct SetList
{
	int  list_count;
	List list;
};

static inline void
set_list_init(SetList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
set_list_add(SetList* self, Set* set)
{
	list_init(&set->link);
	list_append(&self->list, &set->link);
	self->list_count++;
}
