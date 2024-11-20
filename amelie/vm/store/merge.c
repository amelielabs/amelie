
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
#include <amelie_data.h>
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

static void
merge_encode(Store* store, Timezone* tz, Buf* buf)
{
	auto self = (Merge*)store;
	MergeIterator it;
	merge_iterator_init(&it);
	guard(merge_iterator_free, &it);
	// [[], ...]
	encode_array(buf);
	merge_iterator_open(&it, self);
	while (merge_iterator_has(&it))
	{
		auto row = merge_iterator_at(&it);
		auto set = it.current_it->set;
		if (set->count_columns > 1)
			encode_array(buf);
		for (auto col = 0; col < set->count_columns; col++)
			value_encode(row + col, tz, buf);
		if (set->count_columns > 1)
			encode_array_end(buf);
		merge_iterator_next(&it);
	}
	encode_array_end(buf);
}

static void
merge_export(Store* store, Timezone* tz, Buf* buf)
{
	auto self = (Merge*)store;
	MergeIterator it;
	merge_iterator_init(&it);
	guard(merge_iterator_free, &it);
	merge_iterator_open(&it, self);
	auto first = true;
	while (merge_iterator_has(&it))
	{
		auto row = merge_iterator_at(&it);
		if (! first)
			body_add_comma(buf);
		else
			first = false;
		auto set = it.current_it->set;
		if (set->count_columns > 1)
			buf_write(buf, "[", 1);
		for (auto col = 0; col < set->count_columns; col++)
		{
			if (col > 0)
				body_add_comma(buf);
			body_add(buf, row + col, tz, true, true);
		}
		if (set->count_columns > 1)
			buf_write(buf, "]", 1);
		merge_iterator_next(&it);
	}
}

Merge*
merge_create(bool distinct, int64_t limit, int64_t offset)
{
	Merge* self = am_malloc(sizeof(Merge));
	self->store.free   = merge_free;
	self->store.encode = merge_encode;
	self->store.export = merge_export;
	self->list_count   = 0;
	self->limit        = limit;
	self->offset       = offset;
	self->distinct     = distinct;
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
