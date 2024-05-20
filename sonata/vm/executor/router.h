#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Route  Route;
typedef struct Router Router;

#define PARTITION_MAX 8096

struct Route
{
	int      order;
	Channel* channel;
};

struct Router
{
	Route* set;
	int    set_size;
	Route* map;
	int    map_size;
};

static inline void
router_init(Router* self)
{
	self->set      = NULL;
	self->set_size = 0;
	self->map      = NULL;
	self->map_size = 0;
}

static inline void
router_free(Router* self)
{
	if (self->set)
		so_free(self->set);
	if (self->map)
		so_free(self->map);
}

static inline void
router_create(Router* self, int count)
{
	// routes
	int size = sizeof(Route) * count;
	self->set = so_malloc(size);
	self->set_size = count;
	memset(self->set, 0, size);

	// partition map
	size = sizeof(Route) * PARTITION_MAX;
	self->map      = so_malloc(size);
	self->map_size = PARTITION_MAX;
	memset(self->map, 0, size);
}

hot static inline Route*
router_at(Router* self, int order)
{
	return &self->set[order];
}

hot static inline Route*
router_get(Router* self, uint32_t hash)
{
	int partition = hash % PARTITION_MAX;
	return &self->map[partition];
}
