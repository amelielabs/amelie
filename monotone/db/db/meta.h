#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct MetaIf MetaIf;
typedef struct Meta   Meta;

struct MetaIf
{
	void (*init)(Meta*, void*);
	void (*free)(Meta*);
};

struct Meta
{
	Handle      handle;
	MetaConfig* config;
	MetaIf*     iface;
	void*       iface_data;
};

static inline void
meta_free(Meta* self)
{
	if (self->iface)
		self->iface->free(self);
	if (self->config)
		meta_config_free(self->config);
	mn_free(self);
}

static inline Meta*
meta_allocate(MetaConfig* config, MetaIf* iface)
{
	Meta* self = mn_malloc(sizeof(Meta));
	self->config     = NULL;
	self->iface      = iface;
	self->iface_data = NULL;
	guard(self_guard, meta_free, self);
	self->config = meta_config_copy(config);

	handle_init(&self->handle);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)meta_free);
	return unguard(&self_guard);
}

static inline void
meta_data_init(Meta* self, void* arg)
{
	if (self->iface)
		self->iface->init(self, arg);
}

static inline Meta*
meta_of(Handle* handle)
{
	return (Meta*)handle;
}
