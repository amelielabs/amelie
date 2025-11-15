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
	Union*  distinct_aggs;
	Agg*    aggs;
	int64_t limit;
	int64_t offset;
};

Union* union_create(void);

static inline void
union_set_aggs(Union* self, Agg* aggs)
{
	self->aggs = aggs;
}

static inline void
union_set_distinct_aggs(Union* self, Union* distinct_aggs)
{
	assert(! self->distinct_aggs);
	self->distinct_aggs = distinct_aggs;
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
