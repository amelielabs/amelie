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

typedef struct Mapping Mapping;

typedef enum
{
	MAPPING_RANGE,
	MAPPING_HASH
} MappingType;

struct Mapping
{
	MappingType  type;
	MappingHash  hash;
	MappingRange range;
	Keys*        keys;
};

static inline void
mapping_init(Mapping* self, MappingType type, Keys* keys)
{
	self->type = type;
	self->keys = keys;
	mapping_hash_init(&self->hash, keys);
	mapping_range_init(&self->range, keys);
}

static inline void
mapping_free(Mapping* self)
{
	mapping_hash_free(&self->hash);
}

static inline void
mapping_create(Mapping* self)
{
	if (self->type == MAPPING_HASH)
		mapping_hash_create(&self->hash);
}

hot static inline void
mapping_add(Mapping* self, Part* part)
{
	if (self->type == MAPPING_HASH)
	{
		mapping_hash_add(&self->hash, part);
		return;
	}
	mapping_range_add(&self->range, part);
}

hot static inline void
mapping_remove(Mapping* self, Part* part)
{
	if (self->type == MAPPING_HASH)
	{
		mapping_hash_remove(&self->hash, part);
		return;
	}
	mapping_range_remove(&self->range, part);
}

hot static inline Part*
mapping_map(Mapping* self, Row* row)
{
	if (self->type == MAPPING_HASH)
		return mapping_hash_map(&self->hash, row);
	return mapping_range_map(&self->range, row);
}
