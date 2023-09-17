#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct PartRef PartRef;
typedef struct PartMap PartMap;

struct PartRef
{
	Part* part;
	Lock  lock;
};

struct PartMap
{
	PartRef*    map;
	int         map_size;
	LockerCache cache;
};

static inline void
part_map_init(PartMap* self)
{
	self->map = NULL;
	self->map_size = 0;
	locker_cache_init(&self->cache);
}

static inline void
part_map_free(PartMap* self)
{
	if (self->map)
		mn_free(self->map);
	locker_cache_free(&self->cache);
}

static inline void
part_map_create(PartMap* self, int size)
{
	int allocated = sizeof(PartRef) * size;
	self->map = mn_malloc(allocated);
	self->map_size = size;
	for (int i = 0; i < size; i++)
	{
		auto ref = &self->map[i];
		ref->part = NULL;
		lock_init(&ref->lock, &self->cache);
	}
}

hot static inline void
part_map_set(PartMap* self, Part* part)
{
	int i = part->range_start;
	for (; i < part->range_end; i++)
		self->map[i].part = part;
}

static inline PartRef*
part_map_of(PartMap* self, Part* part)
{
	return &self->map[part->range_start];
}

hot static inline PartRef*
part_map_get_first(PartMap* self)
{
	return part_map_of(self, self->map[0].part);
}

hot static inline PartRef*
part_map_get(PartMap* self, Row* row)
{
	int partition = row->hash % self->map_size;
	auto ref = &self->map[partition];

	// return ref by range_start
	return part_map_of(self, ref->part);
}

static inline PartRef*
part_map_next(PartMap* self, Part* part)
{
	if (part->range_end == self->map_size)
		return NULL;
	auto ref = &self->map[part->range_end];

	// return ref by range_start to use same lock
	return part_map_of(self, ref->part);
}
