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

typedef struct Pod Pod;

struct Pod
{
	Mailbox   queue;
	Consensus consensus;
	TrList    prepared;
	TrCache   cache;
	Task*     task;
};

static inline void
pod_init(Pod* self, Task* task)
{
	self->task = task;
	mailbox_init(&self->queue);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	consensus_init(&self->consensus);
}

static inline void
pod_free(Pod* self)
{
	assert(list_empty(&self->queue.list));
	tr_cache_free(&self->cache);
}

static inline void
pod_send_backend(Pod* self, Msg* msg)
{
	task_send(self->task, msg);
}

static inline void
pod_send(Pod* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}
