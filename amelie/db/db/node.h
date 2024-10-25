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

typedef struct NodeIf NodeIf;
typedef struct Node   Node;

struct NodeIf
{
	void (*create)(Node*);
	void (*free)(Node*);
};

struct Node
{
	Handle      handle;
	Uuid        id;
	NodeIf*     iface;
	void*       iface_arg;
	void*       context;
	Route       route;
	NodeConfig* config;
};

static inline void
node_free(Node* self)
{
	if (self->iface)
		self->iface->free(self);
	if (self->config)
		node_config_free(self->config);
	am_free(self);
}

static inline Node*
node_allocate(NodeConfig* config, Uuid* id, NodeIf* iface, void* iface_arg)
{
	Node* self = am_malloc(sizeof(Node));
	self->id        = *id;
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->context   = NULL;
	self->config    = node_config_copy(config);
	handle_init(&self->handle);
	handle_set_schema(&self->handle, NULL);
	handle_set_name(&self->handle, &self->config->id);
	handle_set_free_function(&self->handle, (HandleFree)node_free);
	route_init(&self->route, NULL);
	return self;
}

static inline Node*
node_of(Handle* handle)
{
	return (Node*)handle;
}

static inline void
node_create(Node* self)
{
	self->iface->create(self);
}
