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

typedef struct Union Union;

struct Union
{
	Store   store;
	List    list;
	int     list_count;
	bool    distinct;
	int*    aggs;
	int64_t limit;
	int64_t offset;
	Union*  child;
};

Union* union_create(bool, int64_t, int64_t);
void   union_add(Union*, Set*);

static inline void
union_assign(Union* self, Union* child)
{
	assert(! self->child);
	self->child = child;
}

static inline void
union_set_aggs(Union* self, int* aggs)
{
	self->aggs = aggs;
}
