#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct View View;

struct View
{
	Handle      handle;
	ViewConfig* config;
};

static inline void
view_free(View* self)
{
	if (self->config)
		view_config_free(self->config);
	so_free(self);
}

static inline View*
view_allocate(ViewConfig* config)
{
	View* self = so_malloc(sizeof(View));
	self->config = NULL;
	guard(view_free, self);
	self->config = view_config_copy(config);

	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)view_free);
	return unguard();
}

static inline View*
view_of(Handle* handle)
{
	return (View*)handle;
}

static inline Columns*
view_columns(View* self)
{
	return &self->config->columns;
}
