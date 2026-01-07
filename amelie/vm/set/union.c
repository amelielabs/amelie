
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
#include <amelie_server>
#include <amelie_tier>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>

static void
union_free(Store* store)
{
	auto self = (Union*)store;
	auto list = (Store**)self->list->start;
	for (auto i = 0; i < self->list_count; i++)
		store_free(list[i]);
	buf_free(self->list);
	am_free(self);
}

static StoreIterator*
union_iterator(Store* store)
{
	auto self = (Union*)store;
	return union_iterator_allocate(self);
}

Union*
union_create(UnionType type)
{
	auto self = (Union*)am_malloc(sizeof(Union));
	store_init(&self->store, STORE_UNION);
	self->store.free     = union_free;
	self->store.iterator = union_iterator;
	self->type           = type;
	self->list           = buf_create();
	self->list_count     = 0;
	self->limit          = INT64_MAX;
	self->offset         = 0;
	return self;
}

void
union_add(Union* self, Store* store)
{
	buf_write(self->list, &store, sizeof(Store*));
	// init store configuration by first added store
	if (! self->list_count)
	{
		store_set(&self->store, store->columns, store->keys, store->keys_order);
	} else
	{
		assert(self->store.columns == store->columns);
		assert(self->store.keys == store->keys);
		assert(store_ordered(&self->store) == store_ordered(store));
	}
	self->list_count++;
}
