#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct PartMap PartMap;

#define PARTITION_MAX 8096

struct PartMap
{
	Route** map;
	Route** list;
	int     list_count;
	Buf     list_data;
};

static inline void
part_map_init(PartMap* self)
{
	self->map        = NULL;
	self->list       = NULL;
	self->list_count = 0;
	buf_init(&self->list_data);
}

static inline void
part_map_free(PartMap* self)
{
	for (int i = 0; i < self->list_count; i++)
		router_unref(self->list[i]);

	buf_free(&self->list_data);
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
	int size = sizeof(Route*) * PARTITION_MAX;
	self->map = am_malloc(size);
	memset(self->map, 0, size);
}

static inline void
part_map_add(PartMap* self, Route* route)
{
	// add route to the mapping if not exists
	for (int i = 0; i < self->list_count; i++)
		if (self->list[i] == route)
			return;
	buf_write(&self->list_data, &route, sizeof(void**));
	self->list_count++;
	self->list = (Route**)self->list_data.start;
	router_ref(route);
}

hot static inline void
part_map_set(PartMap* self, int pos, Route* route)
{
	self->map[pos] = route;
}

hot static inline Route*
part_map_get(PartMap* self, uint32_t hash)
{
	int partition = hash % PARTITION_MAX;
	return self->map[partition];
}
