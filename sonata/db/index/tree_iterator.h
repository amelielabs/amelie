#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct TreeIterator TreeIterator;

struct TreeIterator
{
	Iterator      it;
	TtreeIterator iterator;
	Tree*         tree;
};

always_inline static inline TreeIterator*
tree_iterator_of(Iterator* self)
{
	return (TreeIterator*)self;
}

static inline bool
tree_iterator_open(Iterator* arg, Row* key)
{
	auto self = tree_iterator_of(arg);
	auto tree = self->tree;
	return ttree_iterator_open(&self->iterator, &tree->tree, key);
}

static inline bool
tree_iterator_has(Iterator* arg)
{
	auto self = tree_iterator_of(arg);
	return ttree_iterator_has(&self->iterator);
}

static inline Row*
tree_iterator_at(Iterator* arg)
{
	auto self = tree_iterator_of(arg);
	return ttree_iterator_at(&self->iterator);
}

static inline void
tree_iterator_next(Iterator* arg)
{
	auto self = tree_iterator_of(arg);
	ttree_iterator_next(&self->iterator);
}

static inline void
tree_iterator_close(Iterator* arg)
{
	auto self = tree_iterator_of(arg);
	ttree_iterator_close(&self->iterator);
	so_free(arg);
}

static inline Iterator*
tree_iterator_allocate(Tree* tree)
{
	TreeIterator* self = so_malloc(sizeof(*self));
	self->it.has   = tree_iterator_has;
	self->it.at    = tree_iterator_at;
	self->it.next  = tree_iterator_next;
	self->it.close = tree_iterator_close;
	self->tree     = tree;
	ttree_iterator_init(&self->iterator);
	return &self->it;
}
