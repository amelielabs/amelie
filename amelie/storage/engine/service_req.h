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

typedef struct Action     Action;
typedef struct ServiceReq ServiceReq;

typedef enum
{
	ACTION_NONE,
	ACTION_REFRESH,
	ACTION_WAL_GC
} ActionType;

struct Action
{
	ActionType type;
};

struct ServiceReq
{
	uint64_t id;
	List     link;
	int      current;
	int      actions_count;
	Action   actions[];
};

static inline ServiceReq*
service_req_allocate(uint64_t id, int count, va_list args)
{
	auto self = (ServiceReq*)am_malloc(sizeof(ServiceReq) + sizeof(Action) * count);
	self->id = id;
	self->current = 0;
	self->actions_count = count;
	list_init(&self->link);
	for (int i = 0; i < count; i++)
		self->actions[i].type = va_arg(args, ActionType);
	return self;
}

static inline void
service_req_free(ServiceReq* self)
{
	am_free(self);
}
