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

typedef struct UnionIterator UnionIterator;
typedef struct UnionSet      UnionSet;

struct UnionSet
{
	Set*   set;
	Value* current;
	int    pos;
};

struct UnionIterator
{
	StoreIterator  it;
	UnionSet*      current_it;
	Buf*           list;
	int            list_count;
	int64_t        limit;
	Union*         ref;
	StoreIterator* child;
};

always_inline static inline UnionIterator*
union_iterator_of(StoreIterator* self)
{
	return (UnionIterator*)self;
}

hot static inline Value*
union_iterator_step(UnionIterator* self)
{
	auto list = (UnionSet*)self->list->start;
	if (self->current_it)
	{
		auto current = self->current_it;
		current->pos++;
		if (likely(current->pos < current->set->count_rows))
			current->current = set_row_of(current->set, current->pos);
		else
			current->current = NULL;
		self->current_it = NULL;
	}

	UnionSet* min_iterator = NULL;
	Value*    min = NULL;
	for (auto pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = current->current;
		if (! row)
			continue;

		if (min == NULL)
		{
			min_iterator = current;
			min = row;
			if (! current->set->ordered)
				break;
			continue;
		}

		int rc;
		rc = set_compare(current->set, min, row);
		switch (rc) {
		case 0:
			break;
		case 1:
			min_iterator = current;
			min = row;
			break;
		case -1:
			break;
		}
	}

	self->current_it = min_iterator;
	return min;
}

hot static inline Value*
union_iterator_next_distinct(UnionIterator* self)
{
	Value* first;
	if (likely(self->current_it))
		first = self->current_it->current;
	else
		first = union_iterator_step(self);
	if (unlikely(! first))
		return NULL;

	auto set = self->current_it->set;
	for (;;)
	{
		auto next = union_iterator_step(self);
		if (unlikely(! next))
			break;
		if (! set_compare(set, first, next))
		{
			// merge aggregates (merge duplicates)
			if (self->ref->aggs)
				agg_merge(first, next, set->count_columns, self->ref->aggs);
			continue;
		}
		break;
	}

	if (! self->child)
		return first;

	// merge count(distinct) aggregate states

	// do merge join main set with the child set (distinct) to calculate
	// unique entries for count(distinct) aggs

	// set:       [aggs, keys]
	// set child: [keys, agg_order, expr]
	for (;;)
	{
		auto next = self->child->current;
		if (unlikely(! next))
			break;

		// compare only group by keys
		if (! set_compare_keys_n(first + set->count_columns, next, set->count_keys))
			break;

		// calculate unique count(distinct) values
		int  agg_order = next[set->count_keys].integer;
		auto agg = &first[agg_order];
		if (agg->type == TYPE_INT)
			agg->integer++;
		else
			value_set_int(agg, 1);

		store_iterator_next(self->child);
	}

	return first;
}

hot static inline void
union_iterator_next(StoreIterator* arg)
{
	// apply limit
	auto self = union_iterator_of(arg);
	arg->current = NULL;

	if (self->limit-- <= 0)
	{
		self->current_it = NULL;
		return;
	}

	if (! self->ref->distinct)
	{
		arg->current = union_iterator_step(self);
		return;
	}

	// distinct (skip duplicates)
	arg->current = union_iterator_next_distinct(self);
}

static inline void
union_iterator_close(StoreIterator* arg)
{
	auto self = union_iterator_of(arg);
	if (self->child)
		store_iterator_close(self->child);
	if (self->list)
		buf_free(self->list);
	am_free(arg);
}

hot static inline void
union_iterator_open(UnionIterator* self)
{
	auto ref = self->ref;
	if (! ref->list_count)
		return;

	// prepare iterators
	self->list = buf_create();
	buf_reserve(self->list, sizeof(UnionSet) * ref->list_count);

	auto it = (UnionSet*)self->list->start;
	list_foreach(&ref->list)
	{
		auto set = list_at(Set, link);
		if (set->count == 0)
			continue;
		it->set     = set;
		it->pos     = 0;
		it->current = set_row_of(set, 0);
		it++;
		self->list_count++;
	}

	// set to position
	int64_t offset = ref->offset;
	if (offset == 0)
	{
		self->limit = ref->limit;
		union_iterator_next(&self->it);
		return;
	}

	union_iterator_next(&self->it);

	// apply offset
	while (offset-- > 0 && self->it.current)
		union_iterator_next(&self->it);

	self->limit = ref->limit - 1;
	if (self->limit < 0)
		self->it.current = NULL;
}

static inline StoreIterator*
union_iterator_allocate(Union* ref, StoreIterator* child)
{
	UnionIterator* self = am_malloc(sizeof(*self));
	self->it.next    = union_iterator_next;
	self->it.close   = union_iterator_close;
	self->it.current = NULL;
	self->current_it = NULL;
	self->list       = NULL;
	self->list_count = 0;
	self->limit      = INT64_MAX;
	self->ref        = ref;
	self->child      = child;
	errdefer(union_iterator_close, self);

	union_iterator_open(self);
	return &self->it;
}
