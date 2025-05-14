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
	bool (*depend)(Proc*, Str*, Str*);
};

struct Proc
{
	Relation    rel;
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
	relation_init(&self->rel);
	relation_set_schema(&self->rel, &self->config->schema);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)proc_free);
	return self;
}

static inline Proc*
proc_of(Relation* self)
{
	return (Proc*)self;
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

static inline bool
proc_depend(Proc* self, Str* schema, Str* name)
{
	return self->iface->depend(self, schema, name);
}
