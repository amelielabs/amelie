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

#define MAPPING_MAX 1024

struct Mapping
{
	MappingRange** map;
	List           ranges;
	int            ranges_count;
	Keys*          keys;
};

static inline void
mapping_init(Mapping* self, Keys* keys)
{
	self->keys         = keys;
	self->map          = NULL;
	self->ranges_count = 0;
	list_init(&self->ranges);
}

static inline void
mapping_free(Mapping* self)
{
	list_foreach_safe(&self->ranges)
	{
		auto range = list_at(MappingRange, link);
		mapping_range_free(range);
	}
	if (self->map)
		am_free(self->map);
}

static inline void
mapping_create(Mapping* self)
{
	assert(! self->map);
	auto size = sizeof(MappingRange*) * MAPPING_MAX;
	self->map = am_malloc(size);
	memset(self->map, 0, size);
}

hot static inline void
mapping_add(Mapping* self, Part* part)
{
	if (unlikely(! self->map))
		mapping_create(self);

	// [hash][mapping_range]
	auto pos     = part->object->meta.hash_min;
	auto pos_max = part->object->meta.hash_max;

	auto range = (MappingRange*)self->map[pos];
	if (! range)
	{
		range = mapping_range_allocate(self->keys);
		list_append(&self->ranges, &range->link);
		self->ranges_count++;
	}
	mapping_range_add(range, part);

	for (; pos < pos_max; pos++)
		self->map[pos] = range;
}

hot static inline void
mapping_remove(Mapping* self, Part* part)
{
	auto pos     = part->object->meta.hash_min;
	auto pos_max = part->object->meta.hash_max;

	// last partition free range
	auto range = (MappingRange*)self->map[pos];
	if (! range)
		return;
	mapping_range_remove(range, part);
	if (range->tree_count > 0)
		return;

	// free range
	list_unlink(&range->link);
	self->ranges_count--;
	mapping_range_free(range);

	for (; pos < pos_max; pos++)
		self->map[pos] = NULL;
}

hot static inline Part*
mapping_map(Mapping* self, Row* key)
{
	auto hash_partition = row_hash(key, self->keys) % MAPPING_MAX;
	auto ref = self->map[hash_partition];
	assert(ref);
	return mapping_range_map(ref, key);
}
