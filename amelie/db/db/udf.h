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
	bool (*depends_on)(Udf*, Str*, Str*);
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
udf_allocate(UdfConfig* config, UdfIf* iface, void* iface_arg)
{
	Udf* self = am_malloc(sizeof(Udf));
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->context   = NULL;
	self->config    = udf_config_copy(config);
	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)udf_free);
	return self;
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

static inline bool
udf_depends_on(Udf* self, Str* schema, Str* name)
{
	return self->iface->depends_on(self, schema, name);
}
