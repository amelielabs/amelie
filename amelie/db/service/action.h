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

typedef struct Action Action;

enum
{
	ACTION_NONE,
	ACTION_SYNC,
	ACTION_SYNC_CLOSE,
	ACTION_CHECKPOINT
};

struct Action
{
	int      type;
	bool     in_progress;
	uint64_t id;
	List     link;
};

static inline void
action_init(Action* self)
{
	self->type        = ACTION_NONE;
	self->id          = 0;
	self->in_progress = false;
	list_init(&self->link);
}

static inline Action*
action_allocate(void)
{
	auto self = (Action*)am_malloc(sizeof(Action));
	action_init(self);
	return self;
}

static inline void
action_free(Action* self)
{
	am_free(self);
}
