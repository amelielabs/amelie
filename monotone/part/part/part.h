#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Part Part;

struct Part
{
	Handle      handle;
	PartConfig* config;
	Storage     storage;
	List        link;
};

static inline void
part_free(Part* self)
{
	storage_free(&self->storage);
	if (self->config)
		part_config_free(self->config);
	mn_free(self);
}

static inline Part*
part_allocate(PartConfig* config)
{
	Part* self = mn_malloc(sizeof(Part));
	self->config = NULL;
	storage_init(&self->storage);
	list_init(&self->link);
	guard(self_guard, part_free, self);
	self->config = part_config_copy(config);

	handle_init(&self->handle);
	handle_set_id(&self->handle, &self->config->id);
	handle_set_free_function(&self->handle, (HandleFree)part_free);
	return unguard(&self_guard);
}

static inline Part*
part_of(Handle* handle)
{
	return (Part*)handle;
}

static inline Storage*
part_storage(Part* self)
{
	return &self->storage;
}
