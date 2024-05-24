#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Route  Route;
typedef struct Router Router;

struct Route
{
	int      order;
	Channel* channel;
};

struct Router
{
	Route* set;
	int    set_size;
};

static inline void
router_init(Router* self)
{
	self->set      = NULL;
	self->set_size = 0;
}

static inline void
router_free(Router* self)
{
	if (self->set)
		so_free(self->set);
}

static inline void
router_create(Router* self, int count)
{
	// routes
	int size = sizeof(Route) * count;
	self->set = so_malloc(size);
	self->set_size = count;
	memset(self->set, 0, size);
}

hot static inline Route*
router_at(Router* self, int order)
{
	return &self->set[order];
}
