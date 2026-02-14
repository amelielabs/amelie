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
	List           link;
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
	list_init(&self->link);
	relation_init(&self->rel);
	relation_set_db(&self->rel, NULL);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)storage_free);
	relation_set_rsn(&self->rel, state_rsn_next());
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
