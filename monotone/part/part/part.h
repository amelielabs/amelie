#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Part Part;

struct Part
{
	int         refs;
	PartConfig* config;
	Storage     storage;
	List        link;
};

static inline Part*
part_allocate(PartConfig* config)
{
	Part* self = mn_malloc(sizeof(Part));
	self->refs   = 0;
	self->config = config;
	storage_init(&self->storage);
	list_init(&self->link);
	return self;
}

static inline void
part_free(Part* self)
{
	storage_free(&self->storage);
	if (self->config)
		part_config_free(self->config);
	mn_free(self);
}

static inline void
part_ref(Part* self)
{
	self->refs++;
}

static inline void
part_unref(Part* self)
{
	self->refs--;
}

static inline Storage*
part_storage(Part* self)
{
	return &self->storage;
}
