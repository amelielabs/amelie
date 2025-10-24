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

Union* union_create(void);

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

static inline void
union_add(Union* self, Set* set)
{
	// all set properties must match (keys, columns and order)
	list_append(&self->list, &set->link);
	self->list_count++;
}

static inline void
union_set(Union* self, bool distinct, int64_t limit, int64_t offset)
{
	self->limit    = limit;
	self->offset   = offset;
	self->distinct = distinct;
}
