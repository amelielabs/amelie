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

typedef struct MergeIteratorRef MergeIteratorRef;
typedef struct MergeIterator    MergeIterator;

struct MergeIteratorRef
{
	Iterator* it;
	bool      advance;
};

struct MergeIterator
{
	Iterator  it;
	Iterator* current;
	Buf       list;
	int       list_count;
	Keys*     keys;
	bool      free_sources;
	bool      allocated;
};

static inline void
merge_iterator_add(MergeIterator* self, Iterator* it)
{
	MergeIteratorRef ref =
	{
		.it      = it,
		.advance = false
	};
	buf_write(&self->list, &ref, sizeof(ref));
	self->list_count++;
}

static inline void
merge_iterator_open(MergeIterator* self, Keys* keys)
{
	self->keys = keys;
	iterator_next(&self->it);
}

static inline bool
merge_iterator_has(MergeIterator* self)
{
	return self->current != NULL;
}

static inline Row*
merge_iterator_at(MergeIterator* self)
{
	if (unlikely(self->current == NULL))
		return NULL;
	return iterator_at(self->current);
}

hot static inline void
merge_iterator_next(MergeIterator* self)
{
	auto list = (MergeIteratorRef*)self->list.start;
	for (auto pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		if (current->advance)
		{
			iterator_next(current->it);
			current->advance = false;
		}
	}
	self->current = NULL;

	Iterator* min_iterator = NULL;
	Row*      min = NULL;
	for (auto pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = iterator_at(current->it);
		if (row == NULL)
			continue;
		if (min == NULL)
		{
			current->advance = true;
			min_iterator = current->it;
			min = row;
			continue;
		}
		auto rc = compare(self->keys, min, row);
		if (rc == 0)
		{
			current->advance = true;
		} else
		if (rc > 0)
		{
			for (auto i = 0; i < self->list_count; i++)
				list[i].advance = false;
			current->advance = true;
			min_iterator = current->it;
			min = row;
		}
	}
	self->current = min_iterator;
}

static inline void
merge_iterator_free(MergeIterator* self)
{
	if (self->free_sources && self->list_count > 0)
	{
		auto list = (MergeIteratorRef*)self->list.start;
		for (auto pos = 0; pos < self->list_count; pos++)
			iterator_close(list[pos].it);
	}
	buf_free(&self->list);
	if (self->allocated)
		am_free(self);
}

static inline void
merge_iterator_reset(MergeIterator* self)
{
	self->current    = NULL;
	self->keys       = NULL;
	self->list_count = 0;
	buf_reset(&self->list);
}

static inline void
merge_iterator_init(MergeIterator* self)
{
	self->current      = NULL;
	self->keys         = NULL;
	self->list_count   = 0;
	self->allocated    = false;
	self->free_sources = false;
	buf_init(&self->list);
	auto it = &self->it;
	it->has   = (IteratorHas)merge_iterator_has;
	it->at    = (IteratorAt)merge_iterator_at;
	it->next  = (IteratorNext)merge_iterator_next;
	it->close = (IteratorClose)merge_iterator_free;
}

static inline MergeIterator*
merge_iterator_allocate(bool free_sources)
{
	auto self = (MergeIterator*)am_malloc(sizeof(MergeIterator));
	merge_iterator_init(self);
	self->free_sources = free_sources;
	self->allocated    = true;
	return self;
}
