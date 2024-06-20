#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
tree_iterator_open(TreeIterator* self,
                   Tree*         tree,
                   RowKey*       key)
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
	self->current = tree_at(tree, self->page, self->page_pos)->row;
	return match;
}

static inline void
tree_iterator_open_at(TreeIterator* self,
                      Tree*         tree,
                      TreePos*      pos)
{
	self->current  = NULL;
	self->page     = pos->page;
	self->page_pos = pos->page_pos;
	self->tree    = tree;
	self->current  = tree_at(tree, self->page, self->page_pos)->row;
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
			self->current = tree_at(self->tree, self->page, self->page_pos)->row;
		else
			self->current = NULL;
		return;
	}

	self->current = NULL;
	if (likely((self->page_pos + 1) < self->page->keys_count))
	{
		self->page_pos++;
		self->current = tree_at(self->tree, self->page, self->page_pos)->row;
		return;
	}

	auto prev = self->page;
	self->page_pos = 0;
	self->page     = NULL;

	auto next = rbtree_next(&self->tree->tree, &prev->node);
	if (unlikely(next == NULL))
		return;

	self->page    = container_of(next, TreePage, node);
	self->current = tree_at(self->tree, self->page, 0)->row;
}

static inline Row*
tree_iterator_replace(TreeIterator* self, Row* row)
{
	assert(self->current);
	TreePos pos =
	{
		.page     = self->page,
		.page_pos = self->page_pos
	};
	auto prev = tree_replace(self->tree, &pos, row);
	self->current = row;
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
	auto prev = tree_unset_by(self->tree, &pos);
	self->page     = pos.page;
	self->page_pos = pos.page_pos;
	// keeping current row still pointing to the deleted row
	assert(! self->repositioned);
	self->repositioned = true;
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
tree_iterator_init(TreeIterator* self)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->tree         = NULL;
}

static inline void
tree_iterator_sync(TreeIterator* self,
                   TreePage*     page,
                   TreePage*     page_split,
                   int           pos)
{
	if (self->page != page || !self->current)
		return;

	// replace
	if (self->page_pos == pos)
	{
		// update current page row pointer on replace
		self->current = tree_at(self->tree, self->page, self->page_pos)->row;
		return;
	}

	// insert
	if (page_split)
	{
		if (self->page_pos >= page->keys_count)
		{
			self->page = page_split;
			self->page_pos = self->page_pos - page->keys_count;
		}
	}

	// move position, if key was inserted before it
	if (pos <= self->page_pos)
		self->page_pos++;

	self->current = tree_at(self->tree, self->page, self->page_pos)->row;
}
