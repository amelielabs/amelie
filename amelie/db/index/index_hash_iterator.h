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

typedef struct IndexHashIterator IndexHashIterator;

struct IndexHashIterator
{
	Iterator     it;
	HashIterator iterator;
	IndexHash*   index;
};

always_inline static inline IndexHashIterator*
index_hash_iterator_of(Iterator* self)
{
	return (IndexHashIterator*)self;
}

static inline bool
index_hash_iterator_open(Iterator* arg, Row* key)
{
	auto self  = index_hash_iterator_of(arg);
	auto match = hash_iterator_open(&self->iterator, key);
	arg->current = hash_iterator_at(&self->iterator);
	return match;
}

static inline void
index_hash_iterator_next(Iterator* arg)
{
	auto self = index_hash_iterator_of(arg);
	hash_iterator_next(&self->iterator);
	arg->current = hash_iterator_at(&self->iterator);
}

static inline void
index_hash_iterator_close(Iterator* arg)
{
	auto self = index_hash_iterator_of(arg);
	hash_iterator_close(&self->iterator);
	am_free(arg);
}

static inline void
index_hash_iterator_init(IndexHashIterator* self, IndexHash* index)
{
	self->index = index;
	hash_iterator_init(&self->iterator, &index->hash);
	iterator_init(&self->it,
	              index_hash_iterator_open,
	              index_hash_iterator_close,
	              index_hash_iterator_next);
}

static inline Iterator*
index_hash_iterator_allocate(IndexHash* index)
{
	IndexHashIterator* self = am_malloc(sizeof(*self));
	index_hash_iterator_init(self, index);
	return &self->it;
}
