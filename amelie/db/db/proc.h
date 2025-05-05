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

typedef struct ProcIf ProcIf;
typedef struct Proc   Proc;

struct ProcIf
{
	void (*prepare)(Proc*);
	void (*free)(Proc*);
};

struct Proc
{
	Handle      handle;
	ProcIf*     iface;
	void*       iface_arg;
	void*       data;
	ProcConfig* config;
};

static inline void
proc_free(Proc* self)
{
	if (self->iface)
		self->iface->free(self);
	if (self->config)
		proc_config_free(self->config);
	am_free(self);
}

static inline Proc*
proc_allocate(ProcConfig* config, ProcIf* iface, void* iface_arg)
{
	Proc* self = am_malloc(sizeof(Proc));
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->data      = NULL;
	self->config    = proc_config_copy(config);
	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)proc_free);
	return self;
}

static inline Proc*
proc_of(Handle* handle)
{
	return (Proc*)handle;
}

static inline Columns*
proc_columns(Proc* self)
{
	return &self->config->columns;
}

static inline void
proc_prepare(Proc* self)
{
	self->iface->prepare(self);
}
