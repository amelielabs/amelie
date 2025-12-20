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

typedef struct Pipeline Pipeline;

struct Pipeline
{
	Mailbox   queue;
	Consensus consensus;
	TrList    prepared;
	TrCache   cache;
	Task*     task;
};

static inline void
pipeline_init(Pipeline* self)
{
	self->task = NULL;
	mailbox_init(&self->queue);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	consensus_init(&self->consensus);
}

static inline void
pipeline_free(Pipeline* self)
{
	assert(list_empty(&self->queue.list));
	tr_cache_free(&self->cache);
}

static inline void
pipeline_set_task(Pipeline* self, Task* task)
{
	self->task = task;
}

static inline void
pipeline_add(Pipeline* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}

static inline void
pipeline_send(Pipeline* self, Msg* msg)
{
	task_send(self->task, msg);
}
