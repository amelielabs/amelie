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

struct Mapping
{
	MappingHash    hash;
	MappingRange   range;
	MappingConfig* config;
	Keys*          keys;
};

static inline void
mapping_init(Mapping* self, MappingConfig* config, Keys* keys)
{
	self->keys   = keys;
	self->config = mapping_config_copy(config);
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
	if (self->config->type == MAPPING_HASH)
		mapping_hash_create(&self->hash);
}

hot static inline Part*
mapping_map(Mapping* self, Row* row)
{
	if (self->config->type == MAPPING_HASH)
		return mapping_hash_map(&self->hash, row);
	return mapping_range_map(&self->range, row);
}
