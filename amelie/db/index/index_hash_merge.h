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
	Iterator      it;
	HashIterator* current_it;
	int           current_it_order;
	Buf           list;
	int           list_count;
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
	auto list = (HashIterator*)self->list.start;
	for (auto i = 0; i < self->list_count; i++)
	{
		hash_iterator_open(&list[i], key);
		if (!self->current_it && hash_iterator_has(&list[i]))
		{
			self->current_it_order = i;
			self->current_it = &list[i];
		}
	}
	return false;
}

static inline bool
index_hash_merge_has(Iterator* arg)
{
	auto self = index_hash_merge_of(arg);
	return self->current_it != NULL;
}

static inline Row*
index_hash_merge_at(Iterator* arg)
{
	auto self = index_hash_merge_of(arg);
	if (unlikely(! self->current_it))
		return NULL;
	return hash_iterator_at(self->current_it);
}

hot static inline void
index_hash_merge_next(Iterator* arg)
{
	auto self = index_hash_merge_of(arg);
	if (unlikely(! self->current_it))
		return;

	hash_iterator_next(self->current_it);
	if (hash_iterator_has(self->current_it))
		return;

	self->current_it = NULL;
	auto list = (HashIterator*)self->list.start;
	for (;;)
	{
		self->current_it_order++;
		if (self->current_it_order < self->list_count)
			break;
		auto at = &list[self->current_it_order];
		if (hash_iterator_has(at))
		{
			self->current_it = at;
			break;
		}
	}
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
	self->it.open  = index_hash_merge_open;
	self->it.has   = index_hash_merge_has;
	self->it.at    = index_hash_merge_at;
	self->it.next  = index_hash_merge_next;
	self->it.close = index_hash_merge_close;
	self->current_it = NULL;
	self->current_it_order = 0;
	self->list_count = 0;
	buf_init(&self->list);
	return &self->it;
}

static inline void
index_hash_merge_add(IndexHashMerge* self, IndexHash* index)
{
	auto it = (HashIterator*)buf_claim(&self->list, sizeof(HashIterator));
	hash_iterator_init(it, &index->hash);
	self->list_count++;
}
