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
	Backend*     workers;
	int          workers_count;
	Executor*    executor;
	FunctionMgr* function_mgr;
	Db*          db;
};

static inline void
backend_mgr_init(BackendMgr*  self,
                 Db*          db,
                 Executor*    executor,
                 FunctionMgr* function_mgr)
{
	self->workers       = NULL;
	self->workers_count = 0;
	self->executor      = executor;
	self->function_mgr  = function_mgr;
	self->db            = db;
}

static inline void
backend_mgr_start(BackendMgr* self, int count, int cpu_from)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = am_malloc(sizeof(Backend) * count);
	int i = 0;
	for (; i < count; i++)
		backend_init(&self->workers[i], self->db, self->executor,
		              self->function_mgr, i);
	if (cpu_from != -1)
	{
		for (i = 0; i < count; i++)
			backend_start(&self->workers[i], cpu_from + i);
	} else
	{
		for (i = 0; i < count; i++)
			backend_start(&self->workers[i], -1);
	}
}

static inline void
backend_mgr_stop(BackendMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
   	{
		backend_stop(&self->workers[i]);
		backend_free(&self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}
