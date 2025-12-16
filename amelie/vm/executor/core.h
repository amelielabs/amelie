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

typedef struct Core Core;

struct Core
{
	int     order;
	TrList  prepared;
	TrCache cache;
	Msg     msg_stop;
	Task*   task;
};

static inline void
core_init(Core* self, Task* task, int order)
{
	self->order = order;
	self->task  = task;
	msg_init(&self->msg_stop, MSG_STOP);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
}

static inline void
core_free(Core* self)
{
	tr_cache_free(&self->cache);
}

static inline void
core_shutdown(Core* self)
{
	task_send(self->task, &self->msg_stop);
}

static inline void
core_send(Core* self, Req* req)
{
	task_send(self->task, &req->msg);
}
