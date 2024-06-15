
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>

void
ttree_init(Ttree* self,
           int    size_page,
           int    size_split,
           Keys*  keys)
{
	self->count       = 0;
	self->count_pages = 0;
	self->size_page   = size_page;
	self->size_split  = size_split;
	self->keys        = keys;
	rbtree_init(&self->tree);
}

always_inline static inline TtreePage*
ttree_page_of(RbtreeNode* node)
{
	return container_of(node, TtreePage, node);
}

static TtreePage*
ttree_page_allocate(Ttree* self)
{
	TtreePage* page;
	page = so_malloc(sizeof(TtreePage) + self->size_page * sizeof(Row*));
	page->rows_count = 0;
	rbtree_init_node(&page->node);
	return page;
}

static inline void
ttree_page_free(TtreePage* self)
{
	for (int i = 0; i < self->rows_count; i++)
		row_free(self->rows[i]);
	so_free(self);
}

always_inline static inline int
ttree_compare(Ttree* self, TtreePage* page, Row* key)
{
	return compare(self->keys, page->rows[0], key);
}

rbtree_free(tree_truncate, ttree_page_free(ttree_page_of(n)));

void
ttree_free(Ttree* self)
{
	self->count       = 0;
	self->count_pages = 0;
	if (self->tree.root)
		tree_truncate(self->tree.root, NULL);
	rbtree_init(&self->tree);
}

hot static inline
rbtree_get(ttree_find, ttree_compare(arg, ttree_page_of(n), key))

hot static inline TtreePage*
ttree_search_page(Ttree* self, Row* key)
{
	if (self->count_pages == 1)
	{
		auto first = rbtree_min(&self->tree);
		return ttree_page_of(first);
	}

	// page[n].min <= key && key < page[n + 1].min
	RbtreeNode* part_ptr = NULL;
	int rc = ttree_find(&self->tree, self, key, &part_ptr);
	assert(part_ptr != NULL);
	if (rc == 1)
	{
		auto prev = rbtree_prev(&self->tree, part_ptr);
		if (prev)
			part_ptr = prev;
	}
	return ttree_page_of(part_ptr);
}

hot static inline int
ttree_search(Ttree* self, TtreePage* page, Row* key, bool* match)
{
	int min = 0;
	int mid = 0;
	int max = page->rows_count - 1;

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

hot static inline bool
ttree_insert(Ttree*      self,
             TtreePage*  page,
             TtreePage** page_split,
             TtreePos*   page_pos,
             Row*        row)
{
	bool match = false;
	int pos = ttree_search(self, page, row, &match);
	if (match)
	{
		// return position
		page_pos->page = page;
		page_pos->page_pos = pos;
		return true;
	}
	if (pos < 0)
		pos = 0;

	// split
	TtreePage* ref = page;
	TtreePage* l   = page;
	TtreePage* r   = NULL;
	if (unlikely(page->rows_count == self->size_page))
	{
		r = ttree_page_allocate(self);
		l->rows_count = self->size_split;
		r->rows_count = self->size_page - self->size_split;
		memmove(&r->rows[0], &l->rows[self->size_split],
		        sizeof(Row*) * r->rows_count);
		if (pos >= l->rows_count)
		{
			ref = r;
			pos = pos - l->rows_count;
		}
	}

	// insert
	int size = (ref->rows_count - pos) * sizeof(Row*);
	if (size > 0)
		memmove(&ref->rows[pos + 1], &ref->rows[pos], size);
	ref->rows[pos] = row;
	ref->rows_count++;
	self->count++;

	*page_split = r;
	return false;
}

hot Row*
ttree_set(Ttree* self, Row* row)
{
	// create root page
	if (self->count_pages == 0)
	{
		auto page = ttree_page_allocate(self);
		page->rows[0] = row;
		page->rows_count++;
		rbtree_set(&self->tree, NULL, 0, &page->node);
		self->count_pages++;
		self->count++;
		return NULL;
	}

	// search page
	auto page = ttree_search_page(self, row);

	// insert into page
	TtreePage* page_split = NULL;
	TtreePos   pos;
	auto exists = ttree_insert(self, page, &page_split, &pos, row);
	if (exists)
	{
		// replace
		auto prev = pos.page->rows[pos.page_pos];
		pos.page->rows[pos.page_pos] = row;
		return prev;
	}

	// update split page
	if (page_split)
	{
		rbtree_set(&self->tree, &page->node, -1, &page_split->node);
		self->count_pages++;
	}
	return NULL;
}

hot Row*
ttree_set_or_get(Ttree* self, Row* row, TtreePos* pos)
{
	// create root page
	if (self->count_pages == 0)
	{
		auto page = ttree_page_allocate(self);
		page->rows[0] = row;
		page->rows_count++;
		rbtree_set(&self->tree, NULL, 0, &page->node);
		self->count_pages++;
		self->count++;
		return NULL;
	}

	// search page
	auto page = ttree_search_page(self, row);

	// insert into page
	TtreePage* page_split = NULL;
	auto exists = ttree_insert(self, page, &page_split, pos, row);
	if (exists)
		return pos->page->rows[pos->page_pos];

	// update split page
	if (page_split)
	{
		rbtree_set(&self->tree, &page->node, -1, &page_split->node);
		self->count_pages++;
	}

	return NULL;
}

static inline void
ttree_unset_by_next(Ttree* self, TtreePos* pos)
{
	auto next = rbtree_next(&self->tree, &pos->page->node);
	if (next)
		pos->page = ttree_page_of(next);
	else
		pos->page = NULL;
	pos->page_pos = 0;
}

Row*
ttree_unset_by(Ttree* self, TtreePos* pos)
{
	// remove row from the page
	auto page = pos->page;
	auto prev = page->rows[pos->page_pos];
	page->rows[pos->page_pos] = NULL;
	page->rows_count--;
	self->count--;

	// remove page, if it becomes empty
	if (page->rows_count == 0)
	{
		// update position to the next page, if any
		ttree_unset_by_next(self, pos);
		rbtree_remove(&self->tree, &page->node);
		self->count_pages--;
		ttree_page_free(page);
		return prev;
	}

	// not the last position
	int size = (page->rows_count - pos->page_pos) * sizeof(Row*);
	if (size > 0)
	{
		memmove(&page->rows[pos->page_pos],
		        &page->rows[pos->page_pos + 1],
		        size);
		return prev;
	}

	// update position to the next page, if any
	ttree_unset_by_next(self, pos);
	return prev;
}

Row*
ttree_unset(Ttree* self, Row* row)
{
	assert(self->count_pages > 0);

	// search page
	TtreePos pos;
	pos.page = ttree_search_page(self, row);

	// remove existing key from a page
	bool match = false;
	pos.page_pos = ttree_search(self, pos.page, row, &match);
	if (! match)
		return NULL;

	return ttree_unset_by(self, &pos);
}

#if 0
Row*
ttree_unset(Ttree* self, Row* row)
{
	assert(self->count_pages > 0);

	// search page
	auto page = ttree_search_page(self, row);

	// remove existing key from a page
	bool match = false;
	int pos = ttree_search(self, page, row, &match);
	if (! match)
		return NULL;

	// remove row from the page
	auto prev = page->rows[pos];
	page->rows[pos] = NULL;
	page->rows_count--;
	self->count--;

	// remove page, if it becomes empty
	if (page->rows_count == 0)
	{
		rbtree_remove(&self->tree, &page->node);
		self->count_pages--;
		ttree_page_free(page);
	} else
	{
		int size = (page->rows_count - pos) * sizeof(Row*);
		if (size > 0)
			memmove(&page->rows[pos], &page->rows[pos + 1], size);
	}

	return prev;
}
#endif

hot bool
ttree_seek(Ttree* self, Row* key, TtreePos* pos)
{
	if (self->count_pages == 0)
		return false;

	if (key == NULL)
	{
		auto first = rbtree_min(&self->tree);
		pos->page = ttree_page_of(first);
		pos->page_pos = 0;
		return false;
	}

	// search page
	pos->page = ttree_search_page(self, key);

	// search inside page
	bool match = false;
	pos->page_pos = ttree_search(self, pos->page, key, &match);
	return match;
}
