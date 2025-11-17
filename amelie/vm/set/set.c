
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>

void
set_free(Set* self, bool with_data)
{
	if (self->distinct_aggs)
		set_free(self->distinct_aggs, true);
	if (with_data) {
		for (auto i = 0; i < self->count; i++)
			value_free(set_value(self, i));
	}
	buf_free(&self->set);
	buf_free(&self->set_index);
	set_hash_free(&self->hash);
	am_free(self);
}

static void
set_free_store(Store* store)
{
	auto self = (Set*)store;
	set_free(self, true);
}

static StoreIterator*
set_iterator(Store* store)
{
	return set_iterator_allocate((Set*)store);
}

Set*
set_create(void)
{
	Set* self = am_malloc(sizeof(Set));
	store_init(&self->store, STORE_SET);
	self->store.free        = set_free_store;
	self->store.iterator    = set_iterator;
	self->ordered           = NULL;
	self->count             = 0;
	self->count_rows        = 0;
	self->count_columns_row = 0;
	self->count_columns     = 0;
	self->count_keys        = 0;
	self->distinct_aggs     = NULL;
	buf_init(&self->set);
	buf_init(&self->set_index);
	set_hash_init(&self->hash);
	list_init(&self->link);
	return self;
}

void
set_prepare(Set*  self, int count_columns, int count_keys,
            bool* ordered)
{
	self->ordered           = ordered;
	self->count             = 0;
	self->count_rows        = 0;
	self->count_columns_row = count_columns + count_keys;
	self->count_columns     = count_columns;
	self->count_keys        = count_keys;
}

void
set_set_distinct_aggs(Set* self, Set* distinct_aggs)
{
	assert(!self->distinct_aggs || !distinct_aggs);
	self->distinct_aggs = distinct_aggs;
}

void
set_reset(Set* self)
{
	for (auto i = 0; i < self->count; i++)
		value_free(set_value(self, i));
	self->ordered           = NULL;
	self->count             = 0;
	self->count_rows        = 0;
	self->count_columns_row = 0;
	self->count_columns     = 0;
	self->count_keys        = 0;
	buf_reset(&self->set);
	buf_reset(&self->set_index);
	set_hash_reset(&self->hash);
	assert(! self->distinct_aggs);
}

hot static int
set_cmp(const void* p1, const void* p2, void* arg)
{
	Set* self = arg;
	auto row_a = set_row(self, *(uint32_t*)p1);
	auto row_b = set_row(self, *(uint32_t*)p2);
	return set_compare(arg, row_a, row_b);
}

hot void
set_sort(Set* self)
{
	qsort_r(self->set_index.start, self->count_rows, sizeof(uint32_t),
	        set_cmp, self);
}

Value*
set_reserve(Set* self)
{
	// save row position, if keys are in use
	uint32_t row = self->count_rows;
	if (self->ordered)
		buf_write(&self->set_index, &row, sizeof(row));

	// reserve row values
	auto values_size = sizeof(Value) * self->count_columns_row;
	auto values = (Value*)buf_emplace(&self->set, values_size);
	memset(values, 0, values_size);

	self->count += self->count_columns_row;
	self->count_rows++;
	return values;
}

hot void
set_add(Set* self, Value* values)
{
	uint32_t row = self->count_rows;
	set_reserve(self);
	for (auto i = 0; i < self->count_columns_row; i++)
		value_copy(set_column(self, row, i), &values[i]);
}

hot int
set_upsert(Set*  self, Value* keys, uint32_t hash_value,
           bool* exists)
{
	// create or resize hash
	auto hash = &self->hash;
	if (unlikely(! hash->hash))
		set_hash_create(hash, 256);

	if (unlikely(self->count_rows >= (hash->hash_size / 2)))
		set_hash_resize(hash);

	// calculate hash value for the keys
	if (hash_value == 0)
	{
		for (int i = 0; i < self->count_keys; i++)
			hash_value = set_hash_value(&keys[i], hash_value);
	}

	// find row in the hashtable
	SetHashRow* ref;
	int pos = hash_value % hash->hash_size;
	for (;;)
	{
		ref = &hash->hash[pos];
		if (ref->row == UINT32_MAX)
			break;
		if (ref->hash == hash_value)
		{
			auto keys_src = set_key(self, ref->row, 0);
			if (set_compare_keys(self, keys_src, keys))
				break;
		}
		pos = (pos + 1) % hash->hash_size;
	}

	if (exists)
		*exists = ref->row != UINT32_MAX;

	// add new row and copy its keys
	if (ref->row == UINT32_MAX)
	{
		ref->hash = hash_value;
		ref->row  = self->count_rows;
		set_reserve(self);
		for (auto i = 0; i < self->count_keys; i++)
			value_copy(set_key(self, ref->row, i), &keys[i]);
	}

	return ref->row;
}

hot int
set_upsert_ptr(Set*  self, Value** keys, uint32_t hash_value,
               bool* exists)
{
	// create or resize hash
	auto hash = &self->hash;
	if (unlikely(! hash->hash))
		set_hash_create(hash, 256);

	if (unlikely(self->count_rows >= (hash->hash_size / 2)))
		set_hash_resize(hash);

	// calculate hash value for the keys
	if (hash_value == 0)
	{
		for (int i = 0; i < self->count_keys; i++)
			hash_value = set_hash_value(keys[i], hash_value);
	}

	// find row in the hashtable
	SetHashRow* ref;
	int pos = hash_value % hash->hash_size;
	for (;;)
	{
		ref = &hash->hash[pos];
		if (ref->row == UINT32_MAX)
			break;
		if (ref->hash == hash_value)
		{
			auto keys_src = set_key(self, ref->row, 0);
			if (set_compare_keys_ptr(self, keys_src, keys))
				break;
		}
		pos = (pos + 1) % hash->hash_size;
	}

	if (exists)
		*exists = ref->row != UINT32_MAX;

	// add new row and copy its keys
	if (ref->row == UINT32_MAX)
	{
		ref->hash = hash_value;
		ref->row  = self->count_rows;
		set_reserve(self);
		for (auto i = 0; i < self->count_keys; i++)
			value_copy(set_key(self, ref->row, i), keys[i]);
	}

	return ref->row;
}
