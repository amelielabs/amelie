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

typedef struct Udf Udf;

typedef void (*UdfFree)(Udf*);

struct Udf
{
	Relation   rel;
	UdfFree    free;
	void*      free_arg;
	void*      data;
	UdfConfig* config;
};

static inline void
udf_free(Udf* self, bool drop)
{
	unused(drop);
	if (self->free)
		self->free(self);
	if (self->config)
		udf_config_free(self->config);
	am_free(self);
}

static inline Udf*
udf_allocate_as(UdfConfig* config, void* data, UdfFree free, void* free_arg)
{
	auto self = (Udf*)am_malloc(sizeof(Udf));
	self->free     = free;
	self->free_arg = free_arg;
	self->data     = data;
	self->config   = config;
	relation_init(&self->rel);
	relation_set_db(&self->rel, &self->config->db);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)udf_free);
	return self;
}

static inline Udf*
udf_allocate(UdfConfig* config, UdfFree free, void* free_arg)
{
	return udf_allocate_as(udf_config_copy(config), NULL, free, free_arg);
}

static inline Udf*
udf_of(Relation* self)
{
	return (Udf*)self;
}
