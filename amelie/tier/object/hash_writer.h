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

typedef struct HashWriter HashWriter;

struct HashWriter
{
	bool     active;
	uint32_t hash_min;
	uint32_t hash_max;
	Keys*    keys;
	Buf      buf;
};

static inline void
hash_writer_init(HashWriter* self)
{
	self->active   = false;
	self->keys     = NULL;
	self->hash_min = UINT32_MAX;
	self->hash_max = 0;
	buf_init(&self->buf);
}

static inline void
hash_writer_free(HashWriter* self)
{
	buf_free(&self->buf);
}

static inline void
hash_writer_reset(HashWriter* self)
{
	self->active   = false;
	self->keys     = NULL;
	self->hash_min = UINT32_MAX;
	self->hash_max = 0;
	if (buf_empty(&self->buf))
		return;
	memset(buf_cstr(&self->buf), 0, sizeof(uint32_t) * UINT16_MAX);
}

static inline void
hash_writer_prepare(HashWriter* self, Keys* keys, bool active)
{
	self->active = active;
	self->keys   = keys;
	if (! active)
		return;
	buf_reserve(&self->buf, sizeof(uint32_t) * UINT16_MAX);
}

hot static inline void
hash_writer_add(HashWriter* self, Row* row)
{
	if (! self->active)
		return;

	// get hash partition
	auto hash_partition = row_hash(row, self->keys) % UINT16_MAX;

	// track min/max
	if (hash_partition < self->hash_min)
		self->hash_min = hash_partition;
	if (hash_partition > self->hash_max)
		self->hash_max = hash_partition;

	// sum all rows per hash partition
	buf_u32(&self->buf)[ hash_partition ] += row_size(row);
}
