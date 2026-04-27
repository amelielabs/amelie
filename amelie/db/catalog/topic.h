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

typedef struct Topic Topic;

struct Topic
{
	Rel          rel;
	int          cdc;
	TopicConfig* config;
};

static inline void
topic_free(Topic* self, bool drop)
{
	unused(drop);
	topic_config_free(self->config);
	am_free(self);
}

static inline void
topic_show(Topic* self, Buf* buf, int flags)
{
	topic_config_write(self->config, buf, flags);
}

static inline Topic*
topic_allocate(TopicConfig* config)
{
	auto self = (Topic*)am_malloc(sizeof(Topic));
	self->cdc    = 0;
	self->config = topic_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_TOPIC);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_id(rel, &self->config->id);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)topic_show);
	rel_set_free(rel, (RelFree)topic_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Topic*
topic_of(Rel* self)
{
	return (Topic*)self;
}
