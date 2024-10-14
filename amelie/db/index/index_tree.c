
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>

always_inline static inline IndexTree*
index_tree_of(Index* self)
{
	return (IndexTree*)self;
}

hot static bool
index_tree_set(Index* arg, Ref* key, Ref* prev)
{
	auto self = index_tree_of(arg);
	return tree_set(&self->tree, key, prev);
}

hot static void
index_tree_update(Index* arg, Ref* key, Ref* prev, Iterator* it)
{
	unused(arg);
	auto tree_it = index_tree_iterator_of(it);
	tree_iterator_replace(&tree_it->iterator, key, prev);
}

hot static void
index_tree_delete(Index* arg, Ref* key, Ref* prev, Iterator* it)
{
	auto self = index_tree_of(arg);
	auto tree_it = index_tree_iterator_of(it);
	memcpy(key, tree_it->iterator.current, self->tree.keys->key_size);
	tree_iterator_delete(&tree_it->iterator, prev);
}

hot static bool
index_tree_delete_by(Index* arg, Ref* key, Ref* prev)
{
	auto self = index_tree_of(arg);
	return tree_unset(&self->tree, key, prev);
}

hot static bool
index_tree_upsert(Index* arg, Ref* key, Iterator* it)
{
	auto self = index_tree_of(arg);
	TreePos pos;
	auto exists = tree_set_or_get(&self->tree, key, &pos);
	auto tree_it = index_tree_iterator_of(it);
	tree_iterator_open_at(&tree_it->iterator, &self->tree, &pos);
	return exists;
}

hot static bool
index_tree_ingest(Index* arg, Ref* key)
{
	auto self = index_tree_of(arg);
	TreePos pos;
	return tree_set_or_get(&self->tree, key, &pos);
}

hot static Iterator*
index_tree_iterator(Index* arg)
{
	auto self = index_tree_of(arg);
	return index_tree_iterator_allocate(self);
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
index_tree_allocate(IndexConfig* config)
{
	auto self = (IndexTree*)am_malloc(sizeof(IndexTree));
	index_init(&self->index, config);
	tree_init(&self->tree, 512, 256, &config->keys);

	auto iface = &self->index.iface;
	iface->set       = index_tree_set;
	iface->update    = index_tree_update;
	iface->delete    = index_tree_delete;
	iface->delete_by = index_tree_delete_by;
	iface->upsert    = index_tree_upsert;
	iface->ingest    = index_tree_ingest;
	iface->iterator  = index_tree_iterator;
	iface->truncate  = index_tree_truncate;
	iface->free      = index_tree_free;
	return &self->index;
}
