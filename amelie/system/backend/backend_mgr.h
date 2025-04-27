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

typedef struct BackendMgr BackendMgr;

struct BackendMgr
{
	Backend**    workers;
	int          workers_count;
	Router       router;
	FunctionMgr* function_mgr;
	Db*          db;
};

static inline void
backend_mgr_init(BackendMgr*  self,
                 Db*          db,
                 FunctionMgr* function_mgr)
{
	self->workers       = NULL;
	self->workers_count = 0;
	self->function_mgr  = function_mgr;
	self->db            = db;
	router_init(&self->router);
}

static inline void
backend_mgr_ensure(BackendMgr* self, int count)
{
	if (self->workers_count >= count)
		return;

	auto workers = (Backend**)am_malloc(sizeof(Backend*) * count);
	Router router;
	router_init(&router);
	router_allocate(&router, count);
	int i = 0;
	for (; i < self->workers_count; i++)
	{
		workers[i] = self->workers[i];
		router.routes[i] = &workers[i]->route;
	}
	auto start = i;
	for (; i < count; i++)
	{
		workers[i] = backend_allocate(self->db, self->function_mgr, i);
		router.routes[i] = &workers[i]->route;
	}

	auto on_error = error_catch(
		i = start;
		for (; i < count; i++)
			backend_start(workers[i]);
	);
	if (on_error)
	{
		i = start;
		for (; i < count; i++)
		{
			backend_stop(workers[i]);
			backend_free(workers[i]);
		}
		router_free(&router);
		am_free(workers);
		rethrow();
	}

	if (self->workers)
		am_free(self->workers);
	self->workers_count = count;
	self->workers       = workers;

	router_free(&self->router);
	self->router = router;

	// set backends
	opt_int_set(&config()->backends, count);
}

static inline void
backend_mgr_stop(BackendMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		backend_stop(self->workers[i]);
		backend_free(self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
	router_free(&self->router);
}

static inline void
backend_mgr_sync(BackendMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
		backend_sync(self->workers[i]);
}
