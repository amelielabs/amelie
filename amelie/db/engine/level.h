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

typedef struct Level Level;

struct Level
{
	Mapping mapping;
	List    list_ram;
	int     list_ram_count;
	List    list_pending;
	int     list_pending_count;
	List    list_service;
	int     list_service_count;
	Tier*   tier;
	List    link;
};

static inline Level*
level_allocate(Tier* tier, Keys* keys)
{
	auto self = (Level*)am_malloc(sizeof(Level));
	self->list_ram_count     = 0;
	self->list_pending_count = 0;
	self->list_service_count = 0;
	self->tier               = tier;
	mapping_init(&self->mapping, keys);
	list_init(&self->list_ram);
	list_init(&self->list_pending);
	list_init(&self->list_service);
	list_init(&self->link);
	return self;
}

static inline void
level_free(Level* self)
{
	am_free(self);
}

static inline bool
level_empty(Level* self)
{
	return !self->list_ram_count &&
	       !self->list_pending_count &&
	       !self->list_service_count;
}

static inline void
level_add(Level* self, Id* id)
{
	switch (id->type) {
	case ID_RAM:
		list_append(&self->list_ram, &id->link);
		self->list_ram_count++;
		break;
	case ID_PENDING:
		list_append(&self->list_pending, &id->link);
		self->list_pending_count++;
		break;
	case ID_SERVICE:
		list_append(&self->list_service, &id->link);
		self->list_service_count++;
		break;
	default:
		abort();
	}
	tier_storage_ref(id->storage);
}

static inline void
level_remove(Level* self, Id* id)
{
	switch (id->type) {
	case ID_RAM:
		self->list_ram_count--;
		break;
	case ID_PENDING:
		self->list_pending_count--;
		break;
	case ID_SERVICE:
		self->list_service_count--;
		break;
	default:
		abort();
	}
	list_unlink(&id->link);
	tier_storage_unref(id->storage);
}
