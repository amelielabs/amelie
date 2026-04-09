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
	PubSlot    slot;
	SubConfig* config;
	List       link;
};

static inline Sub*
sub_allocate(SubConfig* config)
{
	auto self = (Sub*)am_malloc(sizeof(Sub));
	self->config = sub_config_copy(config);
	pub_slot_init(&self->slot);
	list_init(&self->link);
	return self;
}

static inline void
sub_free(Sub* self)
{
	sub_config_free(self->config);
	am_free(self);
}
