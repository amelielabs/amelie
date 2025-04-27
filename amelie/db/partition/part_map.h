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

typedef struct PartMap PartMap;
typedef struct Part    Part;

#define PARTITION_MAX 8096

struct PartMap
{
	Part** map;
};

static inline void
part_map_init(PartMap* self)
{
	self->map = NULL;
}

static inline void
part_map_free(PartMap* self)
{
	if (self->map)
		am_free(self->map);
}

static inline bool
part_map_created(PartMap* self)
{
	return self->map != NULL;
}

static inline void
part_map_create(PartMap* self)
{
	assert(! self->map);
	int size = sizeof(Part*) * PARTITION_MAX;
	self->map = am_malloc(size);
	memset(self->map, 0, size);
}

hot static inline void
part_map_set(PartMap* self, int pos, Part* part)
{
	self->map[pos] = part;
}

hot static inline Part*
part_map_get(PartMap* self, uint32_t hash)
{
	int partition = hash % PARTITION_MAX;
	return self->map[partition];
}
