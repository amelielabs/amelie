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

typedef struct Track Track;

struct Track
{
	Mailbox   queue;
	uint64_t  seq;
	TrList    prepared;
	TrCache   cache;
	Consensus consensus_pod;
	Consensus consensus;
	bool      pending;
	Consensus pending_consensus;
	Track*    pending_link;
	Task*     backend;
};

static inline void
track_init(Track* self)
{
	self->seq          = 1;
	self->pending      = false;
	self->pending_link = NULL;
	self->backend      = NULL;
	mailbox_init(&self->queue);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	consensus_init(&self->pending_consensus);
	consensus_init(&self->consensus_pod);
	consensus_init(&self->consensus);
}

static inline void
track_free(Track* self)
{
	assert(list_empty(&self->queue.list));
	tr_cache_free(&self->cache);
}

static inline void
track_set_backend(Track* self, Task* task)
{
	self->backend = task;
}

static inline Msg*
track_read(Track* self)
{
	return mailbox_pop(&self->queue, am_self());
}

static inline void
track_write(Track* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}

static inline void
track_send(Track* self, Msg* msg)
{
	task_send(self->backend, msg);
}
