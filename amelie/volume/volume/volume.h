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

typedef struct Volume Volume;

struct Volume
{
	List          list;
	int           list_count;
	VolumeConfig* config;
	Tier*         tier;
	List          link;
};

static inline void
volume_free(Volume* self)
{
	if (self->config)
		volume_config_free(self->config);
	am_free(self);
}

static inline Volume*
volume_allocate(VolumeConfig* config, Tier* tier)
{
	auto self = (Volume*)am_malloc(sizeof(Volume));
	self->config     = volume_config_copy(config);
	self->tier       = tier;
	self->list_count = 0;
	list_init(&self->list);
	return self;
}

static inline void
volume_add(Volume* self, Part* part)
{
	list_append(&self->list, &part->link_volume);
	self->list_count++;
	part_set_volume(part, self);
}

static inline void
volume_remove(Volume* self, Part* part)
{
	assert(part->volume == self);
	assert(part->source == self->tier->config);
	list_unlink(&part->link_volume);
	self->list_count--;
	part_set_volume(part, NULL);
}
