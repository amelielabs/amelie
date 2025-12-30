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

typedef struct Tier Tier;

struct Tier
{
	Relation rel;
	Source*  config;
	int      refs;
	List     link;
};

static inline void
tier_free(Tier* self)
{
	source_free(self->config);
	am_free(self);
}

static inline Tier*
tier_allocate(Source* config)
{
	auto self = (Tier*)am_malloc(sizeof(Tier));
	self->config = source_copy(config);
	self->refs   = 0;
	list_init(&self->link);
	relation_init(&self->rel);
	relation_set_db(&self->rel, NULL);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)tier_free);
	return self;
}

static inline Tier*
tier_of(Relation* self)
{
	return (Tier*)self;
}
