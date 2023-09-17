#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct MetaIface MetaIface;
typedef struct Meta      Meta;

struct MetaIface
{
	void (*init)(Meta*, void*);
	void (*free)(Meta*);
};

struct Meta
{
	Handle      handle;
	MetaConfig* config;
	MetaIface*  iface;
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
meta_allocate(MetaConfig* config, MetaIface* iface, void* iface_arg)
{
	Meta* self = mn_malloc(sizeof(Meta));
	self->config     = NULL;
	self->iface      = iface;
	self->iface_data = NULL;
	handle_init(&self->handle);
	guard(self_guard, meta_free, self);

	self->config = meta_config_copy(config);
	self->handle.name = &self->config->name;
	if (iface)
		self->iface->init(self, iface_arg);

	return unguard(&self_guard);
}

static inline void
meta_destructor(Handle* handle, void *arg)
{
	unused(arg);
	auto self = (Meta*)handle;
	meta_free(self);
}

static inline Meta*
meta_of(Handle* handle)
{
	return (Meta*)handle;
}
