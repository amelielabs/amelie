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
	List     link;
};

struct Router
{
	List list;
	int  list_count;
};

static inline void
route_init(Route* self, Channel* channel)
{
	self->order   = 0;
	self->channel = channel;
	list_init(&self->link);
}

static inline void
router_init(Router* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
router_reorder(Router* self)
{
	int order = 0;
	list_foreach(&self->list)
	{
		auto ref = list_at(Route, link);
		ref->order = order++;
	}
}

static inline void
router_add(Router* self, Route* route)
{
	list_append(&self->list, &route->link);
	self->list_count++;
	router_reorder(self);
}

static inline void
router_del(Router* self, Route* route)
{
	list_unlink(&route->link);
	self->list_count--;
	router_reorder(self);
}

static inline Route*
router_first(Router* self)
{
	return container_of(list_first(&self->list), Route, link);
}
