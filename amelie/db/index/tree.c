
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>

void
tree_init(Tree* self,
          int   size_page,
          int   size_split,
          Keys* keys)
{
	self->count       = 0;
	self->count_pages = 0;
	self->size_page   = size_page;
	self->size_split  = size_split;
	self->keys        = keys;
	rbtree_init(&self->tree);
}

always_inline static inline TreePage*
tree_of(RbtreeNode* node)
{
	return container_of(node, TreePage, node);
}

always_inline static inline void
tree_move(TreePage* dst,
          int       dst_pos,
          TreePage* src,
          int       src_pos, int count)
{
	memmove(&dst->rows[dst_pos], &src->rows[src_pos], count * sizeof(Row*));
}

static TreePage*
tree_allocate(Tree* self)
{
	TreePage* page;
	page = am_malloc(sizeof(TreePage) + self->size_page * sizeof(Row*));
	page->keys_count = 0;
	rbtree_init_node(&page->node);
	return page;
}

static inline void
tree_free_page(Tree* self, TreePage* page)
{
	unused(self);
	am_free(page);
}

rbtree_free(tree_truncate, tree_free_page(arg, tree_of(n)));

void
tree_free(Tree* self)
{
	self->count       = 0;
	self->count_pages = 0;
	if (self->tree.root)
		tree_truncate(self->tree.root, self);
	rbtree_init(&self->tree);
}

always_inline static inline int
tree_compare(Tree* self, TreePage* page, Row* key)
{
	return compare(self->keys, page->rows[0], key);
}

hot static inline
rbtree_get(tree_find, tree_compare(arg, tree_of(n), key))

hot static inline TreePage*
tree_search_page_for_write(Tree*        self,
                           Row*         key,
                           int*         page_rel,
                           RbtreeNode** page_ref)
{
	// page[n].min <= key && key < page[n + 1].min
	*page_rel = tree_find(&self->tree, self, key, page_ref);
	assert(*page_ref != NULL);
	TreePage* page = tree_of(*page_ref);
	if (*page_rel == 1)
	{
		auto prev = rbtree_prev(&self->tree, *page_ref);
		if (prev)
			page = tree_of(prev);
	}
	return page;
}

hot static inline TreePage*
tree_search_page(Tree* self, Row* key)
{
	RbtreeNode* page_ref;
	int page_rel;
	return tree_search_page_for_write(self, key, &page_rel, &page_ref);
}

hot static inline int
tree_search(Tree* self, TreePage* page, Row* key, bool* match)
{
	int min = 0;
	int mid = 0;
	int max = page->keys_count - 1;
	if (compare(self->keys, page->rows[max], key) < 0)
		return max + 1;

	while (max >= min)
	{
		mid = min + ((max - min) >> 1);
		int rc = compare(self->keys, page->rows[mid], key);
		if (rc < 0) {
			min = mid + 1;
		} else
		if (rc > 0) {
			max = mid - 1;
		} else
		{
			*match = true;
			return mid;
		}
	}
	return min;
}

hot static inline TreePage*
tree_set_into(Tree* self, TreePos* pos, Row* key)
{
	auto page = pos->page;
	auto page_pos = pos->page_pos;

	// split
	TreePage* ref = page;
	TreePage* l   = page;
	TreePage* r   = NULL;
	if (unlikely(page->keys_count == self->size_page))
	{
		r = tree_allocate(self);
		l->keys_count = self->size_split;
		r->keys_count = self->size_page - self->size_split;
		tree_move(r, 0, l, self->size_split, r->keys_count);
		if (page_pos >= l->keys_count)
		{
			ref = r;
			page_pos = page_pos - l->keys_count;
		}
	}
	pos->page = ref;
	pos->page_pos = page_pos;

	// insert
	int size = ref->keys_count - page_pos;
	if (size > 0)
		tree_move(ref, page_pos + 1, ref, page_pos, size);
	ref->rows[page_pos] = key;
	ref->keys_count++;
	self->count++;
	return r;
}

hot static inline void
tree_set(Tree* self, TreePos* pos, Row* key)
{
	// insert into page
	auto split = tree_set_into(self, pos, key);
	if (likely(! split))
		return;

	// handle page split
	RbtreeNode* page_ref;
	int page_rel;
	tree_search_page_for_write(self, split->rows[0], &page_rel, &page_ref);
	rbtree_set(&self->tree, page_ref, page_rel, &split->node);
	self->count_pages++;
}

hot bool
tree_upsert(Tree* self, TreePos* pos, Row* key)
{
	// create root page
	if (self->count_pages == 0)
	{
		auto page = tree_allocate(self);
		page->rows[0] = key;
		page->keys_count++;
		rbtree_set(&self->tree, NULL, 0, &page->node);
		self->count_pages++;
		self->count++;
		pos->page = page;
		pos->page_pos = 0;
		return false;
	}

	// search page
	bool exists = false;
	pos->page = tree_search_page(self, key);
	pos->page_pos = tree_search(self, pos->page, key, &exists);
	if (exists)
		return true;
	if (pos->page_pos < 0)
		pos->page_pos = 0;

	// insert into page
	tree_set(self, pos, key);
	return false;
}

hot Row*
tree_replace(Tree* self, Row* key)
{
	TreePos pos;
	if (! tree_upsert(self, &pos, key))
		return NULL;
	auto prev = pos.page->rows[pos.page_pos];
	pos.page->rows[pos.page_pos] = key;
	return prev;
}

static inline void
tree_delete_next(Tree* self, TreePos* pos)
{
	auto next = rbtree_next(&self->tree, &pos->page->node);
	if (next)
		pos->page = tree_of(next);
	else
		pos->page = NULL;
	pos->page_pos = 0;
}

void
tree_delete(Tree* self, TreePos* pos)
{
	// remove row from the page
	auto page = pos->page;
	page->rows[pos->page_pos] = NULL;
	page->keys_count--;
	self->count--;

	// remove page, if it becomes empty
	if (page->keys_count == 0)
	{
		// update position to the next page, if any
		tree_delete_next(self, pos);
		rbtree_remove(&self->tree, &page->node);
		self->count_pages--;
		tree_free_page(self, page);
		return;
	}

	// not the last position
	int size = page->keys_count - pos->page_pos;
	if (size > 0)
	{
		tree_move(page, pos->page_pos, page, pos->page_pos + 1, size);
		return;
	}

	// update position to the next page, if any
	tree_delete_next(self, pos);
}

Row*
tree_delete_by(Tree* self, Row* key)
{
	if (unlikely(! self->count_pages))
		return NULL;

	// search page
	TreePos pos;
	pos.page = tree_search_page(self, key);

	// remove existing key from a page
	bool match = false;
	pos.page_pos = tree_search(self, pos.page, key, &match);
	if (! match)
		return NULL;

	auto prev = pos.page->rows[pos.page_pos];
	tree_delete(self, &pos);
	return prev;
}

hot bool
tree_get(Tree* self, TreePos* pos, Row* key)
{
	if (self->count_pages == 0)
		return false;

	if (key == NULL)
	{
		auto first = rbtree_min(&self->tree);
		pos->page = tree_of(first);
		pos->page_pos = 0;
		return false;
	}

	// search page
	pos->page = tree_search_page(self, key);

	// search inside page
	bool match = false;
	pos->page_pos = tree_search(self, pos->page, key, &match);
	return match;
}
