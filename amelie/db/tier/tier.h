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
	Mapping     mapping;
	List        list_pending;
	int         list_pending_count;
	TierConfig* config;
	List        link;
};

static inline Tier*
tier_allocate(TierConfig* config, Keys* keys)
{
	auto self = (Tier*)am_malloc(sizeof(Tier));
	self->list_pending_count = 0;
	self->config             = config;
	mapping_init(&self->mapping, keys);
	list_init(&self->list_pending);
	list_init(&self->link);
	return self;
}

static inline void
tier_free(Tier* self)
{
	list_foreach_safe(&self->list_pending)
	{
		auto id = list_at(Id, link);
		id_free(id);
	}
	am_free(self);
}

static inline bool
tier_empty(Tier* self)
{
	return !self->list_pending_count;
}

static inline void
tier_add(Tier* self, Id* id)
{
	switch (id->type) {
	case ID_PENDING:
		list_append(&self->list_pending, &id->link);
		self->list_pending_count++;
		break;
	default:
		abort();
	}
	volume_ref(id->volume);
}

static inline void
tier_remove(Tier* self, Id* id)
{
	switch (id->type) {
	case ID_PENDING:
		self->list_pending_count--;
		break;
	default:
		abort();
	}
	list_unlink(&id->link);
	volume_unref(id->volume);
}

static inline void
tier_truncate(Tier* self)
{
	list_foreach_safe(&self->list_pending)
	{
		auto id = list_at(Id, link);
		tier_remove(self, id);
		id_delete(id, id->type);
		id_free(id);
	}
	assert(! self->list_pending_count);
	list_init(&self->list_pending);
}

static inline void
tier_drop(Tier* self)
{
	// delete objects
	tier_truncate(self);

	// delete volumes
	volume_mgr_rmdir(&self->config->volumes);
}


static inline Id*
tier_find(Tier* self, uint64_t psn)
{
	list_foreach_safe(&self->list_pending)
	{
		auto id = list_at(Id, link);
		if (id->id == psn)
			return id;
	}
	return NULL;
}
