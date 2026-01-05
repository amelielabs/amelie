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

typedef struct MappingHash MappingHash;

struct MappingHash
{
	Part** map;
	Keys*  keys;
};

static inline void
mapping_hash_init(MappingHash* self, Keys* keys)
{
	self->map  = NULL;
	self->keys = keys;
}

static inline void
mapping_hash_free(MappingHash* self)
{
	if (self->map)
		am_free(self->map);
}

static inline bool
mapping_hash_created(MappingHash* self)
{
	return self->map != NULL;
}

static inline void
mapping_hash_create(MappingHash* self)
{
	assert(! self->map);
	int size = sizeof(Part*) * UINT16_MAX;
	self->map = am_malloc(size);
	memset(self->map, 0, size);
}

hot static inline void
mapping_hash_set(MappingHash* self, int pos, Part* part)
{
	self->map[pos] = part;
}

hot static inline Part*
mapping_hash_map(MappingHash* self, Row* key)
{
	auto hash_partition = row_hash(key, self->keys) % UINT16_MAX;
	return self->map[hash_partition];
}
