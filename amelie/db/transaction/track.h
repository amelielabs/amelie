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
	Mailbox          queue;
	TrList           prepared;
	TrCache          cache;

	// commited state (partition)
	Consensus        consensus_self;

	// commited (actual global state)
	_Alignas(cache_line)
	ConsensusAtomic  consensus_atomic;
	Consensus        consensus;

	// pending commit state (group commit)
	bool             pending;
	Consensus        pending_consensus;
	Track*           pending_link;
	Task*            backend;
};

static inline void
track_init(Track* self)
{
	self->pending      = false;
	self->pending_link = NULL;
	self->backend      = NULL;
	mailbox_init(&self->queue);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	consensus_init(&self->consensus_self);
	consensus_init(&self->consensus);
	consensus_atomic_init(&self->consensus_atomic);
	consensus_init(&self->pending_consensus);
}

static inline void
track_free(Track* self)
{
	assert(list_empty(&self->queue.list));
	tr_list_reset(&self->prepared, &self->cache);
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

hot static inline void
track_sync(Track* self, Consensus* consensus)
{
	// commit all transactions <= abort
	auto consensus_self = &self->consensus_self;
	auto id = consensus->abort;
	if (unlikely(id > consensus_self->abort))
	{
		tr_abort_list(&self->prepared, &self->cache, id);
		consensus_self->abort = id;
	}

	// commit all transactions <= commit
	id = consensus->commit;
	if (id > consensus_self->commit)
	{
		tr_commit_list(&self->prepared, &self->cache, id);
		consensus_self->commit = id;
	}
}

hot static inline void
track_follow(Track* self, uint64_t id)
{
	// follow transaction id
	auto consensus = &self->consensus;
	if (consensus->commit < id)
		consensus->commit = id;
}
