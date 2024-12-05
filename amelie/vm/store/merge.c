
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>

static void
merge_free(Store* store)
{
	auto self = (Merge*)store;
	auto list = (Set**)self->list.start;
	for (int i = 0; i < self->list_count; i++)
	{
		auto set = list[i];
		store_free(&set->store);
	}
	buf_free(&self->list);
	am_free(self);
}

Merge*
merge_create(bool distinct, int64_t limit, int64_t offset)
{
	Merge* self = am_malloc(sizeof(Merge));
	store_init(&self->store);
	self->store.free = merge_free;
	self->list_count = 0;
	self->limit      = limit;
	self->offset     = offset;
	self->distinct   = distinct;
	buf_init(&self->list);
	return self;
}

void
merge_add(Merge* self, Set* set)
{
	// all set properties must match (keys, columns and order)
	buf_write(&self->list, &set, sizeof(Set**));
	self->list_count++;
}
