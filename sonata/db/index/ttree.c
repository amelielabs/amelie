
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
ttree_of(RbtreeNode* node)
{
	return container_of(node, TtreePage, node);
}

always_inline static inline void
ttree_move(Ttree*     self,
           TtreePage* dst,
           int        dst_pos,
           TtreePage* src,
           int        src_pos, int count)
{
	memmove(ttree_at(self, dst, dst_pos), ttree_at(self, src, src_pos),
	        count * self->keys->key_size);
}

static TtreePage*
ttree_allocate(Ttree* self)
{
	TtreePage* page;
	page = so_malloc(sizeof(TtreePage) + self->size_page * row_key_size(self->keys));
	page->keys_count = 0;
	rbtree_init_node(&page->node);
	return page;
}

static inline void
ttree_free_page(Ttree* self, TtreePage* page)
{
	if (self->keys->primary)
	{
		for (int i = 0; i < page->keys_count; i++)
		{
			auto key = ttree_at(self, page, i);
			row_free(key->row);
		}
	}
	so_free(page);
}

rbtree_free(tree_truncate, ttree_free_page(arg, ttree_of(n)));

void
ttree_free(Ttree* self)
{
	self->count       = 0;
	self->count_pages = 0;
	if (self->tree.root)
		tree_truncate(self->tree.root, self);
	rbtree_init(&self->tree);
}

always_inline static inline int
ttree_compare(Ttree* self, TtreePage* page, RowKey* key)
{
	return compare(self->keys, ttree_at(self, page, 0), key);
}

hot static inline
rbtree_get(ttree_find, ttree_compare(arg, ttree_of(n), key))

hot static inline TtreePage*
ttree_search_page(Ttree* self, RowKey* key)
{
	if (self->count_pages == 1)
	{
		auto first = rbtree_min(&self->tree);
		return ttree_of(first);
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
	return ttree_of(part_ptr);
}

hot static inline int
ttree_search(Ttree* self, TtreePage* page, RowKey* key, bool* match)
{
	int min = 0;
	int mid = 0;
	int max = page->keys_count - 1;
	if (compare(self->keys, ttree_at(self, page, max), key) < 0)
		return max + 1;

	while (max >= min)
	{
		mid = min + ((max - min) >> 1);
		int rc = compare(self->keys, ttree_at(self, page, mid), key);
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
             RowKey*     key)
{
	bool match = false;
	int pos = ttree_search(self, page, key, &match);
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
	if (unlikely(page->keys_count == self->size_page))
	{
		r = ttree_allocate(self);
		l->keys_count = self->size_split;
		r->keys_count = self->size_page - self->size_split;
		ttree_move(self, r, 0, l, self->size_split, r->keys_count);
		if (pos >= l->keys_count)
		{
			ref = r;
			pos = pos - l->keys_count;
		}
	}

	// insert
	int size = ref->keys_count - pos;
	if (size > 0)
		ttree_move(self, ref, pos + 1, ref, pos, size);
	ttree_copy(self, ref, pos, key);
	ref->keys_count++;
	self->count++;

	*page_split = r;
	return false;
}

hot Row*
ttree_set(Ttree* self, Row* row)
{
	auto    key_size = self->keys->key_size;
	uint8_t key_data[key_size];
	auto    key = (RowKey*)key_data;
	row_key_create(key, row, self->keys);

	// create root page
	if (self->count_pages == 0)
	{
		auto page = ttree_allocate(self);
		ttree_copy(self, page, 0, key);
		page->keys_count++;
		rbtree_set(&self->tree, NULL, 0, &page->node);
		self->count_pages++;
		self->count++;
		return NULL;
	}

	// search page
	auto page = ttree_search_page(self, key);

	// insert into page
	TtreePage* page_split = NULL;
	TtreePos   pos;
	auto exists = ttree_insert(self, page, &page_split, &pos, key);
	if (exists)
	{
		// replace
		auto prev = ttree_at(self, page, pos.page_pos)->row;
		ttree_copy(self, page, pos.page_pos, key);
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
	auto    key_size = self->keys->key_size;
	uint8_t key_data[key_size];
	auto    key = (RowKey*)key_data;
	row_key_create(key, row, self->keys);

	// create root page
	if (self->count_pages == 0)
	{
		auto page = ttree_allocate(self);
		ttree_copy(self, page, 0, key);
		page->keys_count++;
		rbtree_set(&self->tree, NULL, 0, &page->node);
		self->count_pages++;
		self->count++;
		return NULL;
	}

	// search page
	auto page = ttree_search_page(self, key);

	// insert into page
	TtreePage* page_split = NULL;
	auto exists = ttree_insert(self, page, &page_split, pos, key);
	if (exists)
		return ttree_at(self, page, pos->page_pos)->row;

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
		pos->page = ttree_of(next);
	else
		pos->page = NULL;
	pos->page_pos = 0;
}

Row*
ttree_unset_by(Ttree* self, TtreePos* pos)
{
	// remove row from the page
	auto page = pos->page;
	auto prev = ttree_at(self, page, pos->page_pos)->row;
	page->keys_count--;
	self->count--;

	// remove page, if it becomes empty
	if (page->keys_count == 0)
	{
		// update position to the next page, if any
		ttree_unset_by_next(self, pos);
		rbtree_remove(&self->tree, &page->node);
		self->count_pages--;
		ttree_free_page(self, page);
		return prev;
	}

	// not the last position
	int size = page->keys_count - pos->page_pos;
	if (size > 0)
	{
		ttree_move(self, page, pos->page_pos, page, pos->page_pos + 1, size);
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
	auto    key_size = self->keys->key_size;
	uint8_t key_data[key_size];
	auto    key = (RowKey*)key_data;
	row_key_create(key, row, self->keys);

	// search page
	TtreePos pos;
	pos.page = ttree_search_page(self, key);

	// remove existing key from a page
	bool match = false;
	pos.page_pos = ttree_search(self, pos.page, key, &match);
	if (! match)
		return NULL;

	return ttree_unset_by(self, &pos);
}

Row*
ttree_replace(Ttree* self, TtreePos* pos, Row* row)
{
	auto    key_size = self->keys->key_size;
	uint8_t key_data[key_size];
	auto    key = (RowKey*)key_data;
	row_key_create(key, row, self->keys);

	auto prev = ttree_at(self, pos->page, pos->page_pos)->row;
	ttree_copy(self, pos->page, pos->page_pos, key);
	return prev;
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
		ttree_free_page(page);
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
ttree_seek(Ttree* self, RowKey* key, TtreePos* pos)
{
	if (self->count_pages == 0)
		return false;

	if (key == NULL)
	{
		auto first = rbtree_min(&self->tree);
		pos->page = ttree_of(first);
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
