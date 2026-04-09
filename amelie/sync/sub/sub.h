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

typedef struct Sub Sub;

struct Sub
{
	Pub*       pub;
	PubSlot    pub_slot;
	SubConfig* config;
	List       link;
};

static inline Sub*
sub_allocate(SubConfig* config)
{
	auto self = (Sub*)am_malloc(sizeof(Sub));
	self->pub    = NULL;
	self->config = sub_config_copy(config);
	pub_slot_init(&self->pub_slot);
	list_init(&self->link);
	return self;
}

static inline void
sub_free(Sub* self)
{
	sub_config_free(self->config);
	am_free(self);
}
