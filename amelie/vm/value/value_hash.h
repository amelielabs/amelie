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
value_hash(Value* self, int type_size, uint32_t hash)
{
	void*   data;
	int     data_size;
	int32_t integer_32;
	if (self->type == TYPE_INT || self->type == TYPE_TIMESTAMP)
	{
		if (type_size == 4)
		{
			integer_32 = self->integer;
			data = &integer_32;
			data_size = sizeof(integer_32);
		} else
		{
			data = &self->integer;
			data_size = sizeof(self->integer);
		}
	} else
	if (self->type == TYPE_UUID)
	{
		data = &self->uuid;
		data_size = sizeof(self->uuid);
	} else
	{
		assert(self->type == TYPE_STRING);
		data = str_u8(&self->string);
		data_size = str_size(&self->string);
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
	if (column->constraints.as_identity)
		self = identity;
	assert(self->type != TYPE_NULL);
	return value_hash(self, column->type_size, hash);
}

hot static inline uint32_t
value_hash_row(Keys*  keys, Value* refs,
               Value* values,
               Value* identity)
{
	// values are row columns
	uint32_t hash = 0;
	list_foreach(&keys->list)
	{
		auto column = list_at(Key, link)->column;
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
	// values are row keys
	uint32_t hash = 0;
	list_foreach(&keys->list)
	{
		auto key = list_at(Key, link);
		auto value = values + key->order;
		hash = value_hash_refs(value, key->column, refs, identity, hash);
	}
	return hash;
}
