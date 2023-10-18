#pragma once

//
// monotone
//
// SQL OLTP database
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
	Route* map;
	int    map_size;
	Route* set;
	int    set_size;
};

static inline void
router_init(Router* self)
{
	self->map      = NULL;
	self->map_size = 0;
	self->set      = NULL;
	self->set_size = 0;
}

static inline void
router_free(Router* self)
{
	if (self->map)
		mn_free(self->map);
	if (self->set)
		mn_free(self->set);
}

static inline void
router_create(Router* self, int count)
{
	// partition map
	int size = sizeof(Route) * PARTITION_MAX;
	self->map      = mn_malloc(size);
	self->map_size = PARTITION_MAX;
	memset(self->map, 0, size);

	// routes
	size = sizeof(Route) * count;
	self->set = mn_malloc(size);
	self->set_size = count;
	memset(self->set, 0, size);
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
