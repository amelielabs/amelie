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

typedef struct MergeIterator MergeIterator;
typedef struct MergeSet      MergeSet;

struct MergeSet
{
	Set*   set;
	Value* current;
	int    pos;
};

struct MergeIterator
{
	StoreIterator it;
	MergeSet*     current_it;
	Buf*          list;
	int           list_count;
	int64_t       limit;
	Merge*        merge;
};

always_inline static inline MergeIterator*
merge_iterator_of(StoreIterator* self)
{
	return (MergeIterator*)self;
}

hot static inline Value*
merge_iterator_step(MergeIterator* self)
{
	auto list = (MergeSet*)self->list->start;
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

	MergeSet* min_iterator = NULL;
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

hot static inline void
merge_iterator_next(StoreIterator* arg)
{
	// apply limit
	auto self = merge_iterator_of(arg);
	if (self->limit-- <= 0)
	{
		self->current_it = NULL;
		arg->current = NULL;
		return;
	}

	// unordered iteration
	if (! self->merge->distinct)
	{
		arg->current = merge_iterator_step(self);
		return;
	}

	// distinct (skip duplicates)
	auto prev = arg->current;
	for (;;)
	{
		arg->current = merge_iterator_step(self);
		if (unlikely(!arg->current || !prev))
			break;
		if (set_compare(self->current_it->set, prev, arg->current) != 0)
			break;
	}
}

static inline void
merge_iterator_close(StoreIterator* arg)
{
	auto self = merge_iterator_of(arg);
	if (self->list)
		buf_free(self->list);
	am_free(arg);
}

hot static inline void
merge_iterator_open(MergeIterator* self)
{
	auto merge = self->merge;
	if (! merge->list_count)
		return;

	// prepare iterators
	self->list = buf_create();
	buf_reserve(self->list, sizeof(MergeSet) * merge->list_count);

	auto it = (MergeSet*)self->list->start;
	list_foreach(&merge->list)
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
	int64_t offset = merge->offset;
	if (offset == 0)
	{
		self->limit = merge->limit;
		merge_iterator_next(&self->it);
		return;
	}

	merge_iterator_next(&self->it);

	// apply offset
	while (offset-- > 0 && self->it.current)
		merge_iterator_next(&self->it);

	self->limit = merge->limit - 1;
	if (self->limit < 0)
		self->it.current = NULL;
}

static inline StoreIterator*
merge_iterator_allocate(Merge* merge)
{
	MergeIterator* self = am_malloc(sizeof(*self));
	self->it.next    = merge_iterator_next;
	self->it.close   = merge_iterator_close;
	self->it.current = NULL;
	self->current_it = NULL;
	self->list       = NULL;
	self->list_count = 0;
	self->limit      = INT64_MAX;
	self->merge      = merge;
	merge_iterator_open(self);
	return &self->it;
}
