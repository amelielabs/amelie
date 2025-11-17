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

typedef struct SetHashRow SetHashRow;
typedef struct SetHash    SetHash;

struct SetHashRow
{
	uint32_t hash;
	uint32_t row;
};

struct SetHash
{
	SetHashRow* hash;
	int         hash_size;
};

static inline void
set_hash_init(SetHash* self)
{
	self->hash      = NULL;
	self->hash_size = 0;
}

static inline void
set_hash_free(SetHash* self)
{
	if (self->hash)
		am_free(self->hash);
	set_hash_init(self);
}

static inline void
set_hash_reset(SetHash* self)
{
	if (self->hash)
		memset(self->hash, 0xff, sizeof(SetHashRow) * self->hash_size);
}

static inline void
set_hash_create(SetHash* self, int size)
{
	auto to_allocate = sizeof(SetHashRow) * size;
	self->hash_size = size;
	self->hash = am_malloc(to_allocate);
	memset(self->hash, 0xff, to_allocate);
}

static inline void
set_hash_put(SetHash* self, SetHashRow* row)
{
	int pos = row->hash % self->hash_size;
	for (;;)
	{
		auto ref = &self->hash[pos];
		if (ref->row == UINT32_MAX) {
			*ref = *row;
			break;
		}
		pos = (pos + 1) % self->hash_size;
	}
}

static inline void
set_hash_resize(SetHash* self)
{
	SetHash hash;
	set_hash_init(&hash);
	set_hash_create(&hash, self->hash_size * 2);
	for (auto pos = 0; pos < self->hash_size; pos++)
	{
		auto ref = &self->hash[pos];
		if (ref->row != UINT32_MAX)
			set_hash_put(&hash, ref);
	}
	set_hash_free(self);
	*self = hash;
}

hot static inline uint32_t
set_hash_value(Value* value, uint32_t hash)
{
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
	return hash ^= hash_fnv(data, data_size);
}
