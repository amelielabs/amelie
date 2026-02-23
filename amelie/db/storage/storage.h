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

typedef struct Storage Storage;

struct Storage
{
	Relation       rel;
	StorageConfig* config;
	int            refs;
};

static inline void
storage_free(Storage* self, bool drop)
{
	unused(drop);
	storage_config_free(self->config);
	am_free(self);
}

static inline Storage*
storage_allocate(StorageConfig* config)
{
	auto self = (Storage*)am_malloc(sizeof(Storage));
	self->config = storage_config_copy(config);
	self->refs   = 0;

	auto rel = &self->rel;
	relation_init(rel);
	relation_set_db(rel, NULL);
	relation_set_name(rel, &self->config->name);
	relation_set_free_function(rel, (RelationFree)storage_free);
	relation_set_rsn(rel, state_rsn_next());
	return self;
}

static inline void
storage_ref(Storage* self)
{
	self->refs++;
}

static inline void
storage_unref(Storage* self)
{
	self->refs--;
	assert(self->refs >= 0);
}

static inline Storage*
storage_of(Relation* self)
{
	return (Storage*)self;
}
