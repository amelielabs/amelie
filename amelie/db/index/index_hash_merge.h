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

typedef struct IndexHashMerge IndexHashMerge;

struct IndexHashMerge
{
	Iterator  it;
	Iterator* current_it;
	int       current_it_order;
	Buf       list;
	int       list_count;
};

always_inline static inline IndexHashMerge*
index_hash_merge_of(Iterator* self)
{
	return (IndexHashMerge*)self;
}

static inline bool
index_hash_merge_open(Iterator* arg, Row* key)
{
	// hash merge iterator does not support point-lookup,
	// this should be done using partition manager
	assert(! key);

	auto self = index_hash_merge_of(arg);
	auto list = (IndexHashIterator*)self->list.start;
	for (auto i = 0; i < self->list_count; i++)
	{
		auto it = &list[i];
		iterator_open(&it->it, it->it.heap, arg->snapshot, NULL);
		if (!self->current_it && iterator_has(&it->it))
		{
			self->current_it_order = i;
			self->current_it = &it->it;
		}
	}

	if (self->current_it)
		arg->current = iterator_at(self->current_it);
	return false;
}

hot static inline void
index_hash_merge_next(Iterator* arg)
{
	auto self = index_hash_merge_of(arg);
	if (unlikely(! self->current_it))
		return;

	iterator_next(self->current_it);
	if (iterator_has(self->current_it))
	{
		arg->current = iterator_at(self->current_it);
		return;
	}

	self->current_it = NULL;
	auto list = (IndexHashIterator*)self->list.start;
	for (;;)
	{
		self->current_it_order++;
		if (self->current_it_order < self->list_count)
			break;
		auto at = &list[self->current_it_order];
		if (iterator_has(&at->it))
		{
			self->current_it = &at->it;
			break;
		}
	}
	if (self->current_it)
		arg->current = iterator_at(self->current_it);
	else
		arg->current = NULL;
}

static inline void
index_hash_merge_close(Iterator* arg)
{
	auto self = index_hash_merge_of(arg);
	buf_free(&self->list);
	am_free(arg);
}

static inline Iterator*
index_hash_merge_allocate(void)
{
	IndexHashMerge* self = am_malloc(sizeof(*self));
	self->current_it       = NULL;
	self->current_it_order = 0;
	self->list_count       = 0;
	buf_init(&self->list);
	iterator_init(&self->it,
	              index_hash_merge_open,
	              index_hash_merge_close,
	              index_hash_merge_next);
	return &self->it;
}

static inline void
index_hash_merge_add(IndexHashMerge* self, IndexHash* index, Heap* heap)
{
	auto it = (IndexHashIterator*)buf_emplace(&self->list, sizeof(IndexHashIterator));
	index_hash_iterator_init(it, index);
	it->it.heap = heap;
	self->list_count++;
}
