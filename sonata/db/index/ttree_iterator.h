#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct TtreeIterator TtreeIterator;

struct TtreeIterator
{
	Row*       current;
	TtreePage* page;
	int        page_pos;
	bool       repositioned;
	Ttree*     ttree;
};

static inline bool
ttree_iterator_open(TtreeIterator* self,
                    Ttree*         ttree,
                    RowKey*        key)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->ttree        = ttree;
	if (unlikely(self->ttree->count == 0))
		return false;

	TtreePos pos;
	bool match = ttree_seek(ttree, key, &pos);
	self->page = pos.page;
	self->page_pos = pos.page_pos;
	if (self->page_pos >= self->page->keys_count)
	{
		self->page     = NULL;
		self->page_pos = 0;
		return false;
	}
	self->current = ttree_at(ttree, self->page, self->page_pos)->row;
	return match;
}

static inline void
ttree_iterator_open_at(TtreeIterator* self,
                       Ttree*         ttree,
                       TtreePos*      pos)
{
	self->current  = NULL;
	self->page     = pos->page;
	self->page_pos = pos->page_pos;
	self->ttree    = ttree;
	self->current  = ttree_at(ttree, self->page, self->page_pos)->row;
}

static inline bool
ttree_iterator_has(TtreeIterator* self)
{
	return self->current != NULL;
}

static inline Row*
ttree_iterator_at(TtreeIterator* self)
{
	return self->current;
}

static inline void
ttree_iterator_next(TtreeIterator* self)
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
			self->current = ttree_at(self->ttree, self->page, self->page_pos)->row;
		else
			self->current = NULL;
		return;
	}

	self->current = NULL;
	if (likely((self->page_pos + 1) < self->page->keys_count))
	{
		self->page_pos++;
		self->current = ttree_at(self->ttree, self->page, self->page_pos)->row;
		return;
	}

	auto prev = self->page;
	self->page_pos = 0;
	self->page     = NULL;

	auto next = rbtree_next(&self->ttree->tree, &prev->node);
	if (unlikely(next == NULL))
		return;

	self->page    = container_of(next, TtreePage, node);
	self->current = ttree_at(self->ttree, self->page, 0)->row;
}

static inline Row*
ttree_iterator_replace(TtreeIterator* self, Row* row)
{
	assert(self->current);
	TtreePos pos =
	{
		.page     = self->page,
		.page_pos = self->page_pos
	};
	auto prev = ttree_replace(self->ttree, &pos, row);
	self->current = row;
	return prev;
}

static inline Row*
ttree_iterator_delete(TtreeIterator* self)
{
	TtreePos pos =
	{
		.page     = self->page,
		.page_pos = self->page_pos
	};
	auto prev = ttree_unset_by(self->ttree, &pos);
	self->page     = pos.page;
	self->page_pos = pos.page_pos;
	// keeping current row still pointing to the deleted row
	assert(! self->repositioned);
	self->repositioned = true;
	return prev;
}

static inline void
ttree_iterator_reset(TtreeIterator* self)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->ttree        = NULL;
}

static inline void
ttree_iterator_close(TtreeIterator* self)
{
	ttree_iterator_reset(self);
}

static inline void
ttree_iterator_init(TtreeIterator* self)
{
	self->current      = NULL;
	self->page         = NULL;
	self->page_pos     = 0;
	self->repositioned = false;
	self->ttree        = NULL;
}

static inline void
ttree_iterator_sync(TtreeIterator* self,
                    TtreePage*     page,
                    TtreePage*     page_split,
                    int            pos)
{
	if (self->page != page || !self->current)
		return;

	// replace
	if (self->page_pos == pos)
	{
		// update current page row pointer on replace
		self->current = ttree_at(self->ttree, self->page, self->page_pos)->row;
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

	self->current = ttree_at(self->ttree, self->page, self->page_pos)->row;
}
