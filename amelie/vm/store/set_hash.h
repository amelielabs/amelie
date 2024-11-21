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
