
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
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>

static void
union_free(Store* store)
{
	auto self = (Union*)store;
	if (self->child)
		union_free(&self->child->store);
	list_foreach_safe(&self->list)
	{
		auto set = list_at(Set, link);
		store_free(&set->store);
	}
	am_free(self);
}

static StoreIterator*
union_iterator(Store* store)
{
	auto self = (Union*)store;
	StoreIterator* child = NULL;
	if (self->child)
		child = union_iterator_allocate(self->child, NULL);
	return union_iterator_allocate(self, child);
}

Union*
union_create(bool distinct, int64_t limit, int64_t offset)
{
	Union* self = am_malloc(sizeof(Union));
	store_init(&self->store, STORE_UNION);
	self->store.free     = union_free;
	self->store.iterator = union_iterator;
	self->list_count     = 0;
	self->limit          = limit;
	self->offset         = offset;
	self->distinct       = distinct;
	self->aggs           = NULL;
	self->child          = NULL;
	list_init(&self->list);
	return self;
}

void
union_add(Union* self, Set* set)
{
	// all set properties must match (keys, columns and order)
	list_append(&self->list, &set->link);
	self->list_count++;
}
