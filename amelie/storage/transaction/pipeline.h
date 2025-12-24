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
	uint64_t  seq;
	TrList    prepared;
	TrCache   cache;
	Consensus consensus;
	Consensus consensus_pod;
	bool      pending;
	Consensus pending_consensus;
	Pipeline* pending_link;
	Task*     backend;
};

static inline void
pipeline_init(Pipeline* self)
{
	self->seq          = 1;
	self->pending      = false;
	self->pending_link = NULL;
	self->backend      = NULL;
	mailbox_init(&self->queue);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	consensus_init(&self->pending_consensus);
	consensus_init(&self->consensus);
	consensus_init(&self->consensus_pod);
}

static inline void
pipeline_free(Pipeline* self)
{
	assert(list_empty(&self->queue.list));
	tr_cache_free(&self->cache);
}

static inline void
pipeline_set_backend(Pipeline* self, Task* task)
{
	self->backend = task;
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
	task_send(self->backend, msg);
}
