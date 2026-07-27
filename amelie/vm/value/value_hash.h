#pragma once

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

hot static inline uint32_t
value_hash(Value* self, Column* column, uint32_t hash)
{
	void*   data;
	int     data_size;
	int32_t i32;
	switch (self->type) {
	case TYPE_INT:
	case TYPE_TIMESTAMP:
	{
		if (column->size == 4)
		{
			i32 = self->integer;
			data = &i32;
			data_size = sizeof(i32);
		} else
		{
			data = &self->integer;
			data_size = sizeof(self->integer);
		}
		break;
	}
	case TYPE_UUID:
	{
		data = &self->uuid;
		data_size = sizeof(self->uuid);
		break;
	}
	case TYPE_STRING:
	{
		data = str_u8(&self->string);
		data_size = str_size(&self->string);
		break;
	}
	default: __builtin_unreachable();
		break;
	}
	return hash_murmur3_32(data, data_size, hash);
}

hot static inline uint32_t
value_hash_refs(Value*   self, Column*  column,
                Value*   refs,
                Value*   identity,
                uint32_t hash)
{
	if (self->type == TYPE_REF)
		self = &refs[self->integer];
	if (self->type == TYPE_NULL && column->constraints.as_identity)
		self = identity;
	assert(self->type != TYPE_NULL);
	return value_hash(self, column, hash);
}

hot static inline uint32_t
value_hash_row(Keys*  keys, Value* refs,
               Value* values,
               Value* identity)
{
	// values are row columns
	uint32_t hash = 0;
	for (auto at = 0; at < keys->count; at++)
	{
		auto key = keys_at(keys, at);
		if (! key->partitioning)
			continue;
		auto column = key->column;
		auto value = values + column->order;
		hash = value_hash_refs(value, column, refs, identity, hash);
	}
	return hash;
}

hot static inline uint32_t
value_hash_keys(Keys*  keys, Value* refs,
                Value* values,
                Value* identity)
{
	// values are partitioning keys
	auto     value = values;
	uint32_t hash  = 0;
	for (auto at = 0; at < keys->count; at++)
	{
		auto key = keys_at(keys, at);
		if (! key->partitioning)
			continue;
		hash = value_hash_refs(value, key->column, refs, identity, hash);
		value++;
	}
	return hash;
}
