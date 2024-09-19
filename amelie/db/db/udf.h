#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct UdfIf UdfIf;
typedef struct Udf   Udf;

struct UdfIf
{
	void (*prepare)(Udf*);
	void (*free)(Udf*);
};

struct Udf
{
	Handle     handle;
	UdfIf*     iface;
	void*      iface_arg;
	void*      context;
	UdfConfig* config;
};

static inline void
udf_free(Udf* self)
{
	if (self->iface)
		self->iface->free(self);
	if (self->config)
		udf_config_free(self->config);
	am_free(self);
}

static inline Udf*
udf_allocate(UdfConfig* config)
{
	Udf* self = am_malloc(sizeof(Udf));
	self->iface     = NULL;
	self->iface_arg = NULL;
	self->context   = NULL;
	self->config    = udf_config_copy(config);
	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)udf_free);
	return self;
}

static inline void
udf_set_iface(Udf*   self,
              UdfIf* iface,
              void*  iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
}

static inline Udf*
udf_of(Handle* handle)
{
	return (Udf*)handle;
}

static inline Columns*
udf_columns(Udf* self)
{
	return &self->config->columns;
}

static inline void
udf_prepare(Udf* self)
{
	self->iface->prepare(self);
}
