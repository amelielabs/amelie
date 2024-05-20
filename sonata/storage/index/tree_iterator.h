#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct TreeIterator TreeIterator;

struct TreeIterator
{
	Iterator    it;
	Row*        current;
	RbtreeNode* pos_next;
	RbtreeNode* pos;
	Tree*       tree;
};

rbtree_get_def(tree_find);

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
	if (unlikely(tree->tree_count == 0))
		return false;
	if (key == NULL)
	{
		self->pos = rbtree_min(&tree->tree);
		self->current = tree_row_of(self->pos);
		return false;
	}

	int rc;
	rc = tree_find(&tree->tree, index_def(&tree->index), key, &self->pos);
	if (self->pos == NULL)
		return false;
	if (rc == -1)
		self->pos = rbtree_next(&tree->tree, self->pos);
	if (self->pos)
		self->current = tree_row_of(self->pos);
	// match
	return rc == 0 && self->pos;
}

static inline bool
tree_iterator_has(Iterator* arg)
{
	return tree_iterator_of(arg)->current != NULL;
}

static inline Row*
tree_iterator_at(Iterator* arg)
{
	return tree_iterator_of(arg)->current;
}

static inline void
tree_iterator_next(Iterator* arg)
{
	auto self = tree_iterator_of(arg);
	if (unlikely(self->current == NULL))
		return;
	self->current = NULL;
	if (self->pos_next)
	{
		self->pos = self->pos_next;
		self->pos_next = NULL;
	} else {
		self->pos = rbtree_next(&self->tree->tree, self->pos);
	}
	if (self->pos)
		self->current = tree_row_of(self->pos);
}

static inline void
tree_iterator_close(Iterator* arg)
{
	auto self = tree_iterator_of(arg);
	self->current  = NULL;
	self->pos      = 0;
	self->pos_next = 0;
	self->tree     = NULL;
	in_free(arg);
}

static inline Iterator*
tree_iterator_allocate(Tree* tree)
{
	TreeIterator* self = in_malloc(sizeof(*self));
	self->it.has   = tree_iterator_has;
	self->it.at    = tree_iterator_at;
	self->it.next  = tree_iterator_next;
	self->it.close = tree_iterator_close;
	self->current  = NULL;
	self->pos      = 0;
	self->pos_next = 0;
	self->tree     = tree;
	return &self->it;
}
