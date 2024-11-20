
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
set_free(Store* store)
{
	auto self = (Set*)store;
	for (auto i = 0; i < self->count; i++)
		value_free(set_value(self, i));
	buf_free(&self->set);
	buf_free(&self->set_index);
	set_hash_free(&self->hash);
	am_free(self->order);
	am_free(self);
}

static void
set_encode(Store* store, Timezone* tz, Buf* buf)
{
	auto self = (Set*)store;
	encode_array(buf);
	for (auto row = 0; row < self->count_rows; row++)
	{
		if (self->count_columns > 1)
			encode_array(buf);
		for (auto col = 0; col < self->count_columns; col++)
			value_encode(set_column_of(self, row, col), tz, buf);
		if (self->count_columns > 1)
			encode_array_end(buf);
	}
	encode_array_end(buf);
}

static void
set_export(Store* store, Timezone* tz, Buf* buf)
{
	auto self = (Set*)store;
	for (auto row = 0; row < self->count_rows; row++)
	{
		if (row > 0)
			body_add_comma(buf);
		if (self->count_columns > 1)
			buf_write(buf, "[", 1);
		for (auto col = 0; col < self->count_columns; col++)
		{
			if (col > 0)
				body_add_comma(buf);
			body_add(buf, set_column_of(self, row, col), tz, true, true);
		}
		if (self->count_columns > 1)
			buf_write(buf, "]", 1);
	}
}

Set*
set_create(int count_columns, int count_keys, uint8_t* order, bool hash)
{
	Set* self = am_malloc(sizeof(Set));
	self->store.free   = set_free;
	self->store.encode = set_encode;
	self->store.export = set_export;
	self->count             = 0;
	self->count_rows        = 0;
	self->count_columns_row = count_columns + count_keys;
	self->count_columns     = count_columns;
	self->count_keys        = count_keys;
	self->order = am_malloc(sizeof(bool) * count_keys);
	if (order)
	{
		self->ordered = true;
		data_read_array(&order);
		for (int i = 0; i < count_keys; i++)
			data_read_bool(&order, &self->order[i]);
	} else
	{
		self->ordered = false;
		for (int i = 0; i < count_keys; i++)
			self->order[i] = true;
	}
	buf_init(&self->set);
	buf_init(&self->set_index);
	set_hash_init(&self->hash);
	if (hash)
		set_hash_create(&self->hash, 256);
	return self;
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
		case VALUE_INT:
		case VALUE_BOOL:
		case VALUE_TIMESTAMP:
			data = &value->integer;
			data_size = sizeof(value->integer);
			break;
		case VALUE_DOUBLE:
			data = &value->dbl;
			data_size = sizeof(value->dbl);
			break;
		case VALUE_STRING:
			data = str_of(&value->string);
			data_size = str_size(&value->string);
			break;
		case VALUE_JSON:
			data = value->data;
			data_size = value->data_size;
			break;
		case VALUE_NULL:
		case VALUE_SET:
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
			if (! set_compare_keys(self, keys_src, keys))
				break;
		}
		pos = (pos + 1) % hash->hash_size;
	}
	return ref;
}

hot Value*
set_get(Set* self, Value* keys, bool add_if_not_exists)
{
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
			return NULL;
		ref->hash = hash;
		ref->row  = set_add_as(self, true, keys);
	}
	return set_row(self, ref->row);
}
