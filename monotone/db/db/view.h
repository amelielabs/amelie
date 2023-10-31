#pragma once

//
// monotone
//
// SQL OLTP database
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
	mn_free(self);
}

static inline View*
view_allocate(ViewConfig* config)
{
	View* self = mn_malloc(sizeof(View));
	self->config = NULL;
	guard(self_guard, view_free, self);
	self->config = view_config_copy(config);

	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)view_free);
	return unguard(&self_guard);
}

static inline View*
view_of(Handle* handle)
{
	return (View*)handle;
}

static inline Def*
view_def(View* self)
{
	return &self->config->def;
}
