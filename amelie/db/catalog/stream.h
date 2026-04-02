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

typedef struct Stream Stream;

struct Stream
{
	Rel           rel;
	StreamConfig* config;
};

static inline void
stream_free(Stream* self, bool drop)
{
	unused(drop);
	stream_config_free(self->config);
	am_free(self);
}

static inline Stream*
stream_allocate(StreamConfig* config)
{
	auto self = (Stream*)am_malloc(sizeof(Stream));
	self->config = stream_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_STREAM);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_free_function(rel, (RelFree)stream_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Stream*
stream_of(Rel* self)
{
	return (Stream*)self;
}
