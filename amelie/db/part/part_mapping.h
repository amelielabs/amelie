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

typedef struct PartMapping PartMapping;

#define PART_MAPPING_MAX 8192

struct PartMapping
{
	Part** map;
	Keys*  keys;
};

static inline void
part_mapping_init(PartMapping* self, Keys* keys)
{
	self->map  = NULL;
	self->keys = keys;
}

static inline void
part_mapping_free(PartMapping* self)
{
	if (self->map)
		am_free(self->map);
}

static inline void
part_mapping_create(PartMapping* self)
{
	assert(! self->map);
	auto size = sizeof(PartMapping*) * PART_MAPPING_MAX;
	self->map = am_malloc(size);
	memset(self->map, 0, size);
}

hot static inline void
part_mapping_add(PartMapping* self, Part* part)
{
	if (unlikely(! self->map))
		part_mapping_create(self);
	auto min = part->heap->header->hash_min;
	auto max = part->heap->header->hash_max;
	for (; min < max; min++)
		self->map[min] = part;
}

hot static inline void
part_mapping_remove(PartMapping* self, Part* part)
{
	auto min = part->heap->header->hash_min;
	auto max = part->heap->header->hash_max;
	for (; min < max; min++)
		self->map[min] = NULL;
}

hot static inline Part*
part_mapping_map(PartMapping* self, Row* key)
{
	auto hash_partition = row_hash(key, self->keys) % PART_MAPPING_MAX;
	return self->map[hash_partition];
}
