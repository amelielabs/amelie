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

typedef struct IndexTreeIterator IndexTreeIterator;

struct IndexTreeIterator
{
	Iterator     it;
	TreeIterator iterator;
	IndexTree*   index;
};

always_inline static inline IndexTreeIterator*
index_tree_iterator_of(Iterator* self)
{
	return (IndexTreeIterator*)self;
}

static inline bool
index_tree_iterator_open(Iterator* arg, Row* key)
{
	auto self = index_tree_iterator_of(arg);
	auto match = tree_iterator_open(&self->iterator, key);
	arg->current = tree_iterator_at(&self->iterator);
	return match;
}

static inline void
index_tree_iterator_next(Iterator* arg)
{
	auto self = index_tree_iterator_of(arg);
	tree_iterator_next(&self->iterator);
	arg->current = tree_iterator_at(&self->iterator);
}

static inline void
index_tree_iterator_close(Iterator* arg)
{
	auto self = index_tree_iterator_of(arg);
	tree_iterator_close(&self->iterator);
	am_free(arg);
}

static inline void
index_tree_iterator_init(IndexTreeIterator* self, IndexTree* index)
{
	self->index = index;
	tree_iterator_init(&self->iterator, &index->tree);
	iterator_init(&self->it,
	              index_tree_iterator_open,
	              index_tree_iterator_close,
	              index_tree_iterator_next);
}

static inline Iterator*
index_tree_iterator_allocate(IndexTree* index)
{
	IndexTreeIterator* self = am_malloc(sizeof(*self));
	index_tree_iterator_init(self, index);
	return &self->it;
}
