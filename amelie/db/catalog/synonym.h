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
	Rel            rel;
	Rel*           ref;
	SynonymConfig* config;
};

static inline void
synonym_free(Synonym* self, bool drop)
{
	unused(drop);
	synonym_config_free(self->config);
	am_free(self);
}

static inline Synonym*
synonym_allocate(SynonymConfig* config)
{
	auto self = (Synonym*)am_malloc(sizeof(Synonym));
	self->config = synonym_config_copy(config);
	self->ref    = NULL;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_SYNONYM);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_free_function(rel, (RelFree)synonym_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Synonym*
synonym_of(Rel* self)
{
	return (Synonym*)self;
}
