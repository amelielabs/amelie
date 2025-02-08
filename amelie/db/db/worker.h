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

typedef struct WorkerIf WorkerIf;
typedef struct Worker   Worker;

struct WorkerIf
{
	void (*create)(Worker*);
	void (*free)(Worker*);
};

struct Worker
{
	Handle        handle;
	Uuid          id;
	WorkerIf*     iface;
	void*         iface_arg;
	void*         context;
	Route         route;
	WorkerConfig* config;
};

static inline void
worker_free(Worker* self)
{
	if (self->iface)
		self->iface->free(self);
	if (self->config)
		worker_config_free(self->config);
	am_free(self);
}

static inline Worker*
worker_allocate(WorkerConfig* config, Uuid* id, WorkerIf* iface, void* iface_arg)
{
	Worker* self = am_malloc(sizeof(Worker));
	self->id        = *id;
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->context   = NULL;
	self->config    = worker_config_copy(config);
	handle_init(&self->handle);
	handle_set_schema(&self->handle, NULL);
	handle_set_name(&self->handle, &self->config->id);
	handle_set_free_function(&self->handle, (HandleFree)worker_free);
	route_init(&self->route, NULL);
	return self;
}

static inline Worker*
worker_of(Handle* handle)
{
	return (Worker*)handle;
}

static inline void
worker_create(Worker* self)
{
	self->iface->create(self);
}
