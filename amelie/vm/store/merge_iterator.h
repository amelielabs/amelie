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

struct MergeIterator
{
	Value*       current;
	SetIterator* current_it;
	Buf          list;
	int          list_count;
	int64_t      limit;
	Merge*       merge;
};

static inline void
merge_iterator_init(MergeIterator* self)
{
	self->current    = NULL;
	self->current_it = NULL;
	self->list_count = 0;
	self->limit      = INT64_MAX;
	self->merge      = NULL;
	buf_init(&self->list);
}

static inline void
merge_iterator_free(MergeIterator* self)
{
	buf_free(&self->list);
}

static inline void
merge_iterator_reset(MergeIterator* self)
{
	self->current    = NULL;
	self->current_it = NULL;
	self->list_count = 0;
	self->limit      = INT64_MAX;
	self->merge      = NULL;
	buf_reset(&self->list);
}

static inline bool
merge_iterator_has(MergeIterator* self)
{
	return self->current != NULL;
}

static inline Value*
merge_iterator_at(MergeIterator* self)
{
	return self->current;
}

hot static inline void
merge_iterator_step(MergeIterator* self)
{
	auto list = (SetIterator*)self->list.start;
	if (self->current_it)
	{
		set_iterator_next(self->current_it);
		self->current_it = NULL;
	}
	self->current = NULL;

	SetIterator* min_iterator = NULL;
	Value*       min = NULL;
	for (int pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = set_iterator_at(current);
		if (row == NULL)
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
	self->current    = min;
}

hot static inline void
merge_iterator_next(MergeIterator* self)
{
	// apply limit
	if (self->limit-- <= 0)
	{
		self->current_it = NULL;
		self->current    = NULL;
		return;
	}

	if (! self->merge->distinct)
	{
		merge_iterator_step(self);
		return;
	}

	// skip duplicates
	auto set = self->current_it->set;
	auto prev = self->current;
	for (;;)
	{
		merge_iterator_step(self);
		auto at = merge_iterator_at(self);
		if (unlikely(!at || !prev))
			break;
		if (set_compare(set, prev, at) != 0)
			break;
	}
}

hot static inline void
merge_iterator_open(MergeIterator* self, Merge* merge)
{
	self->merge = merge;
	if (! merge->list_count)
		return;

	// prepare iterators
	buf_reserve(&self->list, sizeof(SetIterator) * merge->list_count);
	auto it = (SetIterator*)self->list.position;
	auto set = (Set**)merge->list.start;
	for (int i = 0; i < merge->list_count; i++)
	{
		set_iterator_init(it);
		set_iterator_open(it, set[i]);
		it++;
		self->list_count++;
	}

	// set to position
	int64_t offset = merge->offset;
	if (offset == 0)
	{
		self->limit = merge->limit;
		merge_iterator_next(self);
		return;
	}

	merge_iterator_next(self);

	// apply offset
	while (offset-- > 0 && merge_iterator_has(self))
		merge_iterator_next(self);

	self->limit = merge->limit - 1;
	if (self->limit < 0)
		self->current = NULL;
}
