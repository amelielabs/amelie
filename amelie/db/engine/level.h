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
	List    list;
	int     list_count;
	Tier*   tier;
	Level*  next;
};

static inline Level*
level_allocate(Tier* tier, Keys* keys)
{
	auto self = (Level*)am_malloc(sizeof(Level));
	self->tier       = tier;
	self->list_count = 0;
	self->next       = NULL;
	mapping_init(&self->mapping, keys);
	list_init(&self->list);
	return self;
}

static inline void
level_free(Level* self)
{
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	am_free(self);
}

static inline void
level_add(Level* self, Part* part)
{
	list_append(&self->list, &part->link);
	self->list_count++;
}

static inline void
level_remove(Level* self, Part* part)
{
	list_unlink(&part->link);
	self->list_count--;
}
