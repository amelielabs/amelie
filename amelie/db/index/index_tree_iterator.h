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
	return tree_iterator_open(&self->iterator, key);
}

static inline bool
index_tree_iterator_has(Iterator* arg)
{
	auto self = index_tree_iterator_of(arg);
	return tree_iterator_has(&self->iterator);
}

static inline Row*
index_tree_iterator_at(Iterator* arg)
{
	auto self = index_tree_iterator_of(arg);
	return tree_iterator_at(&self->iterator);
}

static inline void
index_tree_iterator_next(Iterator* arg)
{
	auto self = index_tree_iterator_of(arg);
	tree_iterator_next(&self->iterator);
}

static inline void
index_tree_iterator_close(Iterator* arg)
{
	auto self = index_tree_iterator_of(arg);
	tree_iterator_close(&self->iterator);
	am_free(arg);
}

static inline Iterator*
index_tree_iterator_allocate(IndexTree* index)
{
	IndexTreeIterator* self = am_malloc(sizeof(*self));
	self->it.open  = index_tree_iterator_open;
	self->it.has   = index_tree_iterator_has;
	self->it.at    = index_tree_iterator_at;
	self->it.next  = index_tree_iterator_next;
	self->it.close = index_tree_iterator_close;
	self->index    = index;
	tree_iterator_init(&self->iterator, &index->tree);
	return &self->it;
}
