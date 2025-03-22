
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
set_free(Store* store)
{
	auto self = (Set*)store;
	if (self->child)
		set_free(&self->child->store);
	for (auto i = 0; i < self->count; i++)
		value_free(set_value(self, i));
	buf_free(&self->set);
	buf_free(&self->set_index);
	set_hash_free(&self->hash);
	am_free(self);
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
	self->store.free        = set_free;
	self->store.iterator    = set_iterator;
	self->ordered           = NULL;
	self->count             = 0;
	self->count_rows        = 0;
	self->count_columns_row = 0;
	self->count_columns     = 0;
	self->count_keys        = 0;
	self->child             = NULL;
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
	assert(! self->child);
}

void
set_assign(Set* self, Set* child)
{
	assert(!child || !self->child);
	self->child = child;
}

hot static int
set_cmp(const void* p1, const void* p2, void* arg)
{
	Set* self = arg;
	auto row_a = set_row(self, *(uint32_t*)p1);
	auto row_b = set_row(self, *(uint32_t*)p2);
	return set_compare(arg, row_a, row_b);
}

hot static inline int
set_add_as(Set* self, bool keys_only, Value* values)
{
	// save row position, if keys are in use
	uint32_t row = self->count_rows;
	if (self->ordered)
		buf_write(&self->set_index, &row, sizeof(row));

	// reserve and write row values (columns and keys)
	buf_claim(&self->set, sizeof(Value) * self->count_columns_row);

	// copy all values or keys only
	if (keys_only)
	{
		for (auto i = 0; i < self->count_columns; i++)
		{
			auto value = set_column(self, row, i);
			value_init(value);
			value_set_null(value);
		}
		for (auto i = 0; i < self->count_keys; i++)
		{
			auto value = set_key(self, row, i);
			value_init(value);
			value_copy(value, &values[i]);
		}
	} else
	{
		for (auto i = 0; i < self->count_columns_row; i++)
		{
			auto value = set_column(self, row, i);
			value_init(value);
			value_copy(value, &values[i]);
		}
	}

	self->count += self->count_columns_row;
	self->count_rows++;
	return row;
}

hot void
set_add(Set* self, Value* values)
{
	set_add_as(self, false, values);
}

hot static inline uint32_t
set_hash(Set* self, Value* keys)
{
	uint32_t hash = 0;
	for (int i = 0; i < self->count_keys; i++)
	{
		auto  value = &keys[i];
		int   data_size = 0;
		void* data = NULL;
		switch (value->type) {
		case TYPE_INT:
		case TYPE_BOOL:
		case TYPE_TIMESTAMP:
		case TYPE_DATE:
			data = &value->integer;
			data_size = sizeof(value->integer);
			break;
		case TYPE_INTERVAL:
			data = &value->interval;
			data_size = sizeof(value->interval);
			break;
		case TYPE_DOUBLE:
			data = &value->dbl;
			data_size = sizeof(value->dbl);
			break;
		case TYPE_STRING:
			data = str_of(&value->string);
			data_size = str_size(&value->string);
			break;
		case TYPE_JSON:
			data = value->json;
			data_size = value->json_size;
			break;
		case TYPE_VECTOR:
			data = value->vector;
			data_size = vector_size(value->vector);
			break;
		case TYPE_UUID:
			data = &value->uuid;
			data_size = sizeof(&value->uuid);
			break;
		case TYPE_NULL:
			break;
		default:
			error("GROUP BY: unsupported key type");
			break;
		}
		hash ^= hash_fnv(data, data_size);
	}
	return hash;
}

hot static inline SetHashRow*
set_match(Set* self, uint32_t hash_value, Value* keys)
{
	// calculate hash value
	auto hash = &self->hash;
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
	return ref;
}

hot int
set_get(Set* self, Value* keys, bool add_if_not_exists)
{
	if (unlikely(! self->hash.hash))
		set_hash_create(&self->hash, 256);

	if (unlikely(self->count_rows >= (self->hash.hash_size / 2)))
		set_hash_resize(&self->hash);

	// calculate hash value for the keys
	auto hash = set_hash(self, keys);

	// find row in the hashtable
	auto ref = set_match(self, hash, keys);

	// add new row and copy its keys
	if (ref->row == UINT32_MAX)
	{
		if (! add_if_not_exists)
			return -1;
		ref->hash = hash;
		ref->row  = set_add_as(self, true, keys);
	}
	return ref->row;
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
	auto values = (Value*)buf_claim(&self->set, values_size);
	memset(values, 0, values_size);

	self->count += self->count_columns_row;
	self->count_rows++;
	return values;
}

hot void
set_sort(Set* self)
{
	qsort_r(self->set_index.start, self->count_rows, sizeof(uint32_t),
	        set_cmp, self);

	if (self->child)
		set_sort(self->child);
}
