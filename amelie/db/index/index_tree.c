
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

#include <amelie_runtime>
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>

hot static inline void
tree_delta(Tree* self, IndexOp* op, int64_t before)
{
	// update memory usage delta
	int64_t after = self->count_pages;
	if (likely(after == before))
		return;
	int64_t page_delta = after - before;
	int64_t page_bytes = (int64_t)(sizeof(TreePage) + self->size_page * sizeof(Row*));
	op->delta += page_delta * page_bytes;
}

hot static bool
index_tree_upsert(Index* self, IndexOp* op)
{
	auto tree = &index_tree_of(self)->tree;

	// upsert
	auto before = tree->count_pages;
	TreePos pos;
	auto exists = tree_upsert(tree, &pos, op->row);
	tree_delta(tree, op, before);

	// set iterator
	auto it      = op->it;
	auto tree_it = index_tree_iterator_of(it);
	tree_iterator_open_at(&tree_it->iterator, &pos);
	it->current = tree_iterator_at(&tree_it->iterator);
	return exists;
}

hot static bool
index_tree_replace(Index* self, IndexOp* op)
{
	auto tree = &index_tree_of(self)->tree;
	auto before = tree->count_pages;
	if (! op->it)
	{
		// replace by key
		op->row_prev = tree_replace(tree, op->row);
	} else
	{
		// replace by iterator
		auto tree_it = index_tree_iterator_of(op->it);
		op->row_prev = tree_iterator_replace(&tree_it->iterator, op->row);
		op->it->current = tree_iterator_at(&tree_it->iterator);
	}
	tree_delta(tree, op, before);
	return op->row_prev != NULL;
}

hot static bool
index_tree_delete(Index* self, IndexOp* op)
{
	auto tree = &index_tree_of(self)->tree;
	auto before = tree->count_pages;
	if (! op->it)
	{
		// delete by key
		op->row_prev = tree_delete_by(tree, op->row);
	} else
	{
		// delete by iterator
		auto tree_it = index_tree_iterator_of(op->it);
		op->row_prev = tree_iterator_delete(&tree_it->iterator);
		op->it->current = tree_iterator_at(&tree_it->iterator);
	}
	tree_delta(tree, op, before);
	return op->row_prev != NULL;
}

static void
index_tree_truncate(Index* self, IndexOp* op)
{
	auto tree = &index_tree_of(self)->tree;
	auto before = tree->count_pages;
	tree_free(tree);
	tree_delta(tree, op, before);
}

static void
index_tree_free(Index* self, IndexOp* op)
{
	index_tree_truncate(self, op);
	am_free(self);
}

hot static Iterator*
index_tree_iterator(Index* self)
{
	return index_tree_iterator_allocate(index_tree_of(self));
}

hot static Iterator*
index_tree_iterator_merge(Index* self, Iterator* it, Heap* heap)
{
	if (! it)
		it = index_tree_merge_allocate();
	index_tree_merge_add(index_tree_merge_of(it), index_tree_of(self), heap);
	return it;
}

Index*
index_tree_allocate(IndexConfig* config, void* arg)
{
	auto self = (IndexTree*)am_malloc(sizeof(IndexTree));
	index_init(&self->index, config, arg);
	tree_init(&self->tree, 512, 256, &config->keys.comparable);

	auto iface = &self->index.iface;
	iface->upsert         = index_tree_upsert;
	iface->replace        = index_tree_replace;
	iface->delete         = index_tree_delete;
	iface->truncate       = index_tree_truncate;
	iface->free           = index_tree_free;
	iface->iterator       = index_tree_iterator;
	iface->iterator_merge = index_tree_iterator_merge;
	return &self->index;
}
