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

typedef struct ServiceReq ServiceReq;
typedef struct Action     Action;

enum
{
	ACTION_NONE,
	ACTION_FLUSH,
	ACTION_MERGE
};

struct Action
{
	ServiceReq* req;
	int         pending;
	int         type;
	uint64_t    id;
	Event       event;
	List        link;
};

static inline void
action_init(Action* self)
{
	self->req     = NULL;
	self->pending = 0;
	self->type    = ACTION_NONE;
	self->id      = 0;
	event_init(&self->event);
	list_init(&self->link);
}
