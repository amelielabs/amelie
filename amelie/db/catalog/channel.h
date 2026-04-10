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

typedef struct Channel Channel;

struct Channel
{
	Rel            rel;
	ChannelConfig* config;
};

static inline void
channel_free(Channel* self, bool drop)
{
	unused(drop);
	channel_config_free(self->config);
	am_free(self);
}

static inline Channel*
channel_allocate(ChannelConfig* config)
{
	auto self = (Channel*)am_malloc(sizeof(Channel));
	self->config = channel_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_CHANNEL);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_free_function(rel, (RelFree)channel_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Channel*
channel_of(Rel* self)
{
	return (Channel*)self;
}
