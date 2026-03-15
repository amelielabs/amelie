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
	Rel        rel;
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

	// set relation
	auto rel = &self->rel;
	rel_init(rel);
	rel_set_db(rel, &self->config->db);
	rel_set_name(rel, &self->config->name);
	rel_set_free_function(rel, (RelFree)udf_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Udf*
udf_allocate(UdfConfig* config, UdfFree free, void* free_arg)
{
	return udf_allocate_as(udf_config_copy(config), NULL, free, free_arg);
}

static inline Udf*
udf_of(Rel* self)
{
	return (Udf*)self;
}
