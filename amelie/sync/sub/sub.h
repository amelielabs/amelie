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
	Rel        rel;
	PubSlot    slot;
	SubConfig* config;
};

static inline void
sub_free(Sub* self, bool drop)
{
	unused(drop);
	sub_config_free(self->config);
	am_free(self);
}

static inline Sub*
sub_allocate(SubConfig* config)
{
	auto self = (Sub*)am_malloc(sizeof(Sub));
	self->config = sub_config_copy(config);
	pub_slot_init(&self->slot);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_SUBSCRIPTION);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_free_function(rel, (RelFree)sub_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Sub*
sub_of(Rel* self)
{
	return (Sub*)self;
}
