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

typedef struct Commit Commit;

struct Commit
{
	uint64_t  list_max;
	int       list_count;
	List      list;
	Buf       routes;
	int       routes_count;
	WriteList write;
};

static inline void
commit_init(Commit* self)
{
	self->list_max     = 0;
	self->list_count   = 0;
	self->routes_count = 0;
	list_init(&self->list);
	buf_init(&self->routes);
	write_list_init(&self->write);
}

static inline void
commit_free(Commit* self)
{
	buf_free(&self->routes);
}

static inline void
commit_reset(Commit* self)
{
	self->routes_count = var_int_of(&config()->backends);
	buf_reset(&self->routes);
	auto size = sizeof(Route*) * self->routes_count;
	auto routes = buf_claim(&self->routes, size);
	memset(routes, 0, size);

	self->list_max   = 0;
	self->list_count = 0;
	list_init(&self->list);
	write_list_reset(&self->write);
}

hot static inline void
commit_add(Commit* self, Dtr* dtr)
{
	// add transaction to the list
	list_append(&self->list, &dtr->link_commit);
	self->list_count++;

	// track max transaction id
	if (dtr->id > self->list_max)
		self->list_max = dtr->id;

	// create a unique list of all routes
	auto routes = (Route**)self->routes.start;
	auto dispatch = &dtr->dispatch;
	for (auto i = 0; i < dispatch->steps; i++)
	{
		auto step = dispatch_at(dispatch, i);
		list_foreach(&step->list.list)
		{
			auto req = list_at(Req, link);
			auto route = req->arg.route;
			if (! routes[route->id])
				routes[route->id] = route;
		}
	}
}
