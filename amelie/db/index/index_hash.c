
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
hash_delta(Hash* self, IndexOp* op, int64_t before)
{
	// update memory usage delta
	int64_t after = hash_size(self);
	if (likely(after == before))
		return;
	op->delta += sizeof(Row*) * (after - before);
}

hot static bool
index_hash_upsert(Index* self, IndexOp* op)
{
	auto hash = &index_hash_of(self)->hash;

	// upsert
	auto before = hash_size(hash);
	uint64_t pos = 0;
	auto exists = hash_get_or_set(hash, op->row, &pos);
	hash_delta(hash, op, before);

	// set iterator
	auto it      = op->it;
	auto hash_it = index_hash_iterator_of(it);
	hash_iterator_open_at(&hash_it->iterator, pos);
	it->current = hash_iterator_at(&hash_it->iterator);
	return exists;
}

hot static bool
index_hash_replace(Index* self, IndexOp* op)
{
	auto hash = &index_hash_of(self)->hash;
	auto before = hash_size(hash);
	if (! op->it)
	{
		// replace by key
		op->row_prev = hash_set(hash, op->row);
	} else
	{
		// replace by iterator
		auto hash_it = index_hash_iterator_of(op->it);
		op->row_prev = hash_iterator_replace(&hash_it->iterator, op->row);
		op->it->current = hash_iterator_at(&hash_it->iterator);
	}
	hash_delta(hash, op, before);
	return op->row_prev != NULL;
}

hot static bool
index_hash_delete(Index* self, IndexOp* op)
{
	auto hash = &index_hash_of(self)->hash;
	auto before = hash_size(hash);
	if (! op->it)
	{
		// delete by key
		op->row_prev = hash_delete(hash, op->row);
	} else
	{
		// delete by iterator
		auto hash_it = index_hash_iterator_of(op->it);
		op->row_prev = hash_iterator_delete(&hash_it->iterator);
		op->it->current = hash_iterator_at(&hash_it->iterator);
	}
	hash_delta(hash, op, before);
	return op->row_prev != NULL;
}

static void
index_hash_truncate(Index* self, IndexOp* op)
{
	auto hash = &index_hash_of(self)->hash;
	auto comparable = hash->comparable;
	auto before = hash_size(hash);
	hash_free(hash);
	auto size = self->config->size;
	if (! size)
		size = 256;
	hash_create(hash, comparable, size);
	hash_delta(hash, op, before);
}

static void
index_hash_create(Index* self, IndexOp* op)
{
	auto hash = &index_hash_of(self)->hash;
	auto before = hash_size(hash);
	auto size = self->config->size;
	if (! size)
		size = 256;
	hash_create(hash, &self->config->keys.comparable, size);
	hash_delta(hash, op, before);
}

static void
index_hash_free(Index* self, IndexOp* op)
{
	auto hash = &index_hash_of(self)->hash;
	auto before = hash_size(hash);
	hash_free(hash);
	hash_delta(hash, op, before);

	am_free(self);
}

hot static Iterator*
index_hash_iterator(Index* self)
{
	return index_hash_iterator_allocate(index_hash_of(self));
}

hot static Iterator*
index_hash_iterator_merge(Index* self, Iterator* it, Heap* heap)
{
	if (! it)
		it = index_hash_merge_allocate();
	index_hash_merge_add(index_hash_merge_of(it), index_hash_of(self), heap);
	return it;
}

Index*
index_hash_allocate(IndexConfig* config, void* arg)
{
	auto self = (IndexHash*)am_malloc(sizeof(IndexHash));
	index_init(&self->index, config, arg);
	hash_init(&self->hash);

	auto iface = &self->index.iface;
	iface->upsert         = index_hash_upsert;
	iface->replace        = index_hash_replace;
	iface->delete         = index_hash_delete;
	iface->truncate       = index_hash_truncate;
	iface->create         = index_hash_create;
	iface->free           = index_hash_free;
	iface->iterator       = index_hash_iterator;
	iface->iterator_merge = index_hash_iterator_merge;
	return &self->index;
}

uint64_t
index_hash_size(IndexConfig* config)
{
	auto size = config->size;
	if (! size)
		size = 256;
	return sizeof(Row*) * size;
}
