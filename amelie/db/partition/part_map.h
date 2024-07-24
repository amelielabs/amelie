#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct PartMap PartMap;

#define PARTITION_MAX 8096

struct PartMap
{
	Route** map;
	int     map_size;
};

static inline void
part_map_init(PartMap* self)
{
	self->map      = NULL;
	self->map_size = 0;
}

static inline void
part_map_free(PartMap* self)
{
	if (self->map)
		so_free(self->map);
}

static inline void
part_map_create(PartMap* self)
{
	assert(! self->map);
	int size = sizeof(Route*) * PARTITION_MAX;
	self->map      = so_malloc(size);
	self->map_size = PARTITION_MAX;
	memset(self->map, 0, size);
}

hot static inline void
part_map_set(PartMap* self, int pos, Route* route)
{
	self->map[pos] = route;
	router_ref(route);
}

hot static inline void
part_map_unset(PartMap* self, int pos)
{
	auto route = self->map[pos];
	if (! route)
		return;
	self->map[pos] = NULL;
	router_unref(route);
}

hot static inline Route*
part_map_get(PartMap* self, uint32_t hash)
{
	int partition = hash % PARTITION_MAX;
	return self->map[partition];
}

static inline void
part_map_unmap(PartMap* self)
{
	for (int i = 0; i < self->map_size; i++)
		part_map_unset(self, i);
}
