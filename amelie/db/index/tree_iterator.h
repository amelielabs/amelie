#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct TreeIterator TreeIterator;

struct TreeIterator
{
	Ref*      current;
	TreePage* page;
	int       page_pos;
	bool      repositioned;
	Tree*     tree;
};

static inline bool
tree_iterator_open(TreeIterator* self, Tree* tree, Ref* key)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->tree        = tree;
	if (unlikely(self->tree->count == 0))
		return false;

	TreePos pos;
	bool match = tree_seek(tree, key, &pos);
	self->page = pos.page;
	self->page_pos = pos.page_pos;
	if (self->page_pos >= self->page->keys_count)
	{
		self->page     = NULL;
		self->page_pos = 0;
		return false;
	}
	self->current = tree_at(tree, self->page, self->page_pos);
	return match;
}

static inline void
tree_iterator_open_at(TreeIterator* self, Tree* tree, TreePos* pos)
{
	self->current  = NULL;
	self->page     = pos->page;
	self->page_pos = pos->page_pos;
	self->tree     = tree;
	self->current  = tree_at(tree, self->page, self->page_pos);
}

static inline bool
tree_iterator_has(TreeIterator* self)
{
	return self->current != NULL;
}

static inline Ref*
tree_iterator_at(TreeIterator* self)
{
	return self->current;
}

static inline void
tree_iterator_next(TreeIterator* self)
{
	if (unlikely(self->current == NULL))
		return;

	// cursor already repositioned to the next row after
	// previous delete operation
	if (self->repositioned)
	{
		self->repositioned = false;

		// update current row pointer
		if (self->page)
			self->current = tree_at(self->tree, self->page, self->page_pos);
		else
			self->current = NULL;
		return;
	}

	self->current = NULL;
	if (likely((self->page_pos + 1) < self->page->keys_count))
	{
		self->page_pos++;
		self->current = tree_at(self->tree, self->page, self->page_pos);
		return;
	}

	auto prev = self->page;
	self->page_pos = 0;
	self->page     = NULL;

	auto next = rbtree_next(&self->tree->tree, &prev->node);
	if (unlikely(next == NULL))
		return;

	self->page    = container_of(next, TreePage, node);
	self->current = tree_at(self->tree, self->page, 0);
}

static inline void
tree_iterator_replace(TreeIterator* self, Ref* key, Ref* prev)
{
	assert(self->current);
	TreePos pos =
	{
		.page     = self->page,
		.page_pos = self->page_pos
	};
	tree_copy_from(self->tree, pos.page, pos.page_pos, prev);
	tree_copy(self->tree, pos.page, pos.page_pos, key);
	self->current = key;
}

static inline void
tree_iterator_delete(TreeIterator* self, Ref* prev)
{
	TreePos pos =
	{
		.page     = self->page,
		.page_pos = self->page_pos
	};
	tree_copy_from(self->tree, pos.page, pos.page_pos, prev);
	tree_unset_by(self->tree, &pos);
	self->page     = pos.page;
	self->page_pos = pos.page_pos;
	assert(! self->repositioned);
	self->repositioned = true;

	// keeping current row still pointing to the deleted row
	self->current  = prev;
}

static inline void
tree_iterator_reset(TreeIterator* self)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->tree         = NULL;
}

static inline void
tree_iterator_close(TreeIterator* self)
{
	tree_iterator_reset(self);
}

static inline void
tree_iterator_init(TreeIterator* self)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->tree         = NULL;
}
