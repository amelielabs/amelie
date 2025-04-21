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

typedef struct TreeIterator TreeIterator;

struct TreeIterator
{
	Row*      current;
	TreePage* page;
	int       page_pos;
	bool      repositioned;
	Tree*     tree;
};

static inline bool
tree_iterator_open(TreeIterator* self, Row* key)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	if (unlikely(self->tree->count == 0))
		return false;

	TreePos pos;
	bool match = tree_get(self->tree, &pos, key);
	self->page = pos.page;
	self->page_pos = pos.page_pos;
	if (self->page_pos >= self->page->keys_count)
	{
		self->page     = NULL;
		self->page_pos = 0;
		return false;
	}
	self->current = self->page->rows[self->page_pos];
	return match;
}

static inline void
tree_iterator_open_at(TreeIterator* self, TreePos* pos)
{
	self->current  = NULL;
	self->page     = pos->page;
	self->page_pos = pos->page_pos;
	self->current  = self->page->rows[self->page_pos];
}

static inline bool
tree_iterator_has(TreeIterator* self)
{
	return self->current != NULL;
}

static inline Row*
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
			self->current = self->page->rows[self->page_pos];
		else
			self->current = NULL;
		return;
	}

	self->current = NULL;
	if (likely((self->page_pos + 1) < self->page->keys_count))
	{
		self->page_pos++;
		self->current = self->page->rows[self->page_pos];
		return;
	}

	auto prev = self->page;
	self->page_pos = 0;
	self->page     = NULL;

	auto next = rbtree_next(&self->tree->tree, &prev->node);
	if (unlikely(next == NULL))
		return;

	self->page    = container_of(next, TreePage, node);
	self->current = self->page->rows[0];
}

static inline Row*
tree_iterator_replace(TreeIterator* self, Row* key)
{
	assert(self->current);
	TreePos pos =
	{
		.page     = self->page,
		.page_pos = self->page_pos
	};
	auto prev = pos.page->rows[pos.page_pos];
	pos.page->rows[pos.page_pos] = key;
	self->current = key;
	return prev;
}

static inline Row*
tree_iterator_delete(TreeIterator* self)
{
	TreePos pos =
	{
		.page     = self->page,
		.page_pos = self->page_pos
	};
	auto prev = pos.page->rows[pos.page_pos];
	tree_delete(self->tree, &pos);
	self->page     = pos.page;
	self->page_pos = pos.page_pos;
	assert(! self->repositioned);
	self->repositioned = true;

	// keeping current row still pointing to the deleted row
	self->current  = prev;
	return prev;
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
tree_iterator_init(TreeIterator* self, Tree* tree)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->tree         = tree;
}
