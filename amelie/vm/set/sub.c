
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
#include <amelie_db>
#include <amelie_sync>
#include <amelie_value.h>
#include <amelie_set.h>

static void
sub_store_free(Store* store)
{
	auto self = (SubStore*)store;
	am_free(self);
}

static StoreIterator*
sub_store_iterator(Store* store)
{
	auto self = (SubStore*)store;
	return sub_store_iterator_allocate(self->sub, self->cdc);
}

SubStore*
sub_store_create(Sub* sub, Cdc* cdc)
{
	auto self = (SubStore*)am_malloc(sizeof(SubStore));
	store_init(&self->store, STORE_SUB);
	self->sub = sub;
	self->cdc = cdc;
	self->store.free     = sub_store_free;
	self->store.iterator = sub_store_iterator;
	return self;
}
