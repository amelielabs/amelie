
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

always_inline static inline IndexTree*
index_tree_of(Index* self)
{
	return (IndexTree*)self;
}

hot static bool
index_tree_upsert(Index* arg, Row* key, Iterator* it)
{
	auto self = index_tree_of(arg);
	TreePos pos;
	auto exists = tree_upsert(&self->tree, &pos, key);
	auto tree_it = index_tree_iterator_of(it);
	tree_iterator_open_at(&tree_it->iterator, &pos);
	return exists;
}

hot static Row*
index_tree_replace_by(Index* arg, Row* key)
{
	auto self = index_tree_of(arg);
	return tree_replace(&self->tree, key);
}

hot static Row*
index_tree_replace(Index* arg, Row* key, Iterator* it)
{
	unused(arg);
	auto tree_it = index_tree_iterator_of(it);
	return tree_iterator_replace(&tree_it->iterator, key);
}

hot static Row*
index_tree_delete_by(Index* arg, Row* key)
{
	auto self = index_tree_of(arg);
	return tree_delete_by(&self->tree, key);
}

hot static Row*
index_tree_delete(Index* arg, Iterator* it)
{
	unused(arg);
	auto tree_it = index_tree_iterator_of(it);
	return tree_iterator_delete(&tree_it->iterator);
}

hot static Iterator*
index_tree_iterator(Index* arg)
{
	auto self = index_tree_of(arg);
	return index_tree_iterator_allocate(self);
}

hot static Iterator*
index_tree_iterator_merge(Index* arg, Iterator* it)
{
	auto self = index_tree_of(arg);
	if (! it)
		it = index_tree_merge_allocate();
	index_tree_merge_add(index_tree_merge_of(it), self);
	return it;
}

static void
index_tree_truncate(Index* arg)
{
	auto self = index_tree_of(arg);
	tree_free(&self->tree);
}

static void
index_tree_free(Index* arg)
{
	auto self = index_tree_of(arg);
	tree_free(&self->tree);
	am_free(self);
}

Index*
index_tree_allocate(IndexConfig* config, Heap* heap)
{
	auto self = (IndexTree*)am_malloc(sizeof(IndexTree));
	index_init(&self->index, config, heap);
	tree_init(&self->tree, 512, 256, &config->keys);

	auto iface = &self->index.iface;
	iface->upsert         = index_tree_upsert;
	iface->replace_by     = index_tree_replace_by;
	iface->replace        = index_tree_replace;
	iface->delete_by      = index_tree_delete_by;
	iface->delete         = index_tree_delete;
	iface->iterator       = index_tree_iterator;
	iface->iterator_merge = index_tree_iterator_merge;
	iface->truncate       = index_tree_truncate;
	iface->free           = index_tree_free;
	return &self->index;
}
