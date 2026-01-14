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
	MAPPING_HASH,
	MAPPING_HASH_RANGE
} MappingType;

struct Mapping
{
	MappingType type;
	void**      map;
	List        ranges;
	int         ranges_count;
	Keys*       keys;
};

static inline void
mapping_init(Mapping* self, MappingType type, Keys* keys)
{
	self->type         = type;
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
	auto size = sizeof(void*) * UINT16_MAX;
	self->map = am_malloc(size);
	memset(self->map, 0, size);
}

hot static inline void
mapping_add(Mapping* self, Part* part)
{
	if (! self->map)
		mapping_create(self);

	// [hash][mapping_range]
	auto pos     = part->object->meta.hash_min;
	auto pos_max = part->object->meta.hash_max;
	auto ref     = NULL;
	if (self->type == MAPPING_HASH_RANGE)
	{
		auto range = (MappingRange*)self->map[pos];
		if (! range)
		{
			range = mapping_range_allocate(self->keys);
			list_append(&self->ranges, &range->link);
			self->ranges_count++;
		}
		mapping_range_add(range, part);
		ref = range;
	} else {
		ref = part;
	}
	for (; pos < pos_max; pos++)
		self->map[pos] = ref;
}

hot static inline void
mapping_remove(Mapping* self, Part* part)
{
	auto pos     = part->object->meta.hash_min;
	auto pos_max = part->object->meta.hash_max;
	if (self->type == MAPPING_HASH_RANGE)
	{
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
	}
	for (; pos < pos_max; pos++)
		self->map[pos] = NULL;
}

hot static inline Part*
mapping_map(Mapping* self, Row* key)
{
	auto hash_partition = row_hash(key, self->keys) % UINT16_MAX;
	auto ref = self->map[hash_partition];
	if (self->type == MAPPING_HASH)
		return ref;
	if (! ref)
		return NULL;
	return mapping_range_map(ref, key);
}
