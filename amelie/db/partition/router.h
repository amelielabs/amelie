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

typedef struct Route  Route;
typedef struct Router Router;

struct Route
{
	int      order;
	Channel* channel;
};

struct Router
{
	Route** routes;
	int     routes_count;
};

static inline void
route_init(Route* self, Channel* channel, int order)
{
	self->order   = order;
	self->channel = channel;
}

static inline void
router_init(Router* self)
{
	self->routes = NULL;
	self->routes_count = 0;
}

static inline void
router_free(Router* self)
{
	if (self->routes)
	{
		am_free(self->routes);
		self->routes = NULL;
	}
	self->routes_count = 0;
}

static inline void
router_allocate(Router* self, int count)
{
	auto size = sizeof(Router*) * count;
	self->routes = am_malloc(size);
	self->routes_count = count;
	memset(self->routes, 0, size);
}
