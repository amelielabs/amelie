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

typedef struct Synonym Synonym;

struct Synonym
{
	Relation       rel;
	Relation*      ref;
	SynonymConfig* config;
};

static inline void
synonym_free(Synonym* self, bool drop)
{
	unused(drop);
	// todo: unref?
	synonym_config_free(self->config);
	am_free(self);
}

static inline Synonym*
synonym_allocate(SynonymConfig* config)
{
	auto self = (Synonym*)am_malloc(sizeof(Synonym));
	self->config = synonym_config_copy(config);
	self->ref    = NULL;
	relation_init(&self->rel);
	relation_set_db(&self->rel, &self->config->db);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)synonym_free);
	relation_set_rsn(&self->rel, state_rsn_next());
	return self;
}

static inline Synonym*
synonym_of(Relation* self)
{
	return (Synonym*)self;
}
