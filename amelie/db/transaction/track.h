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
	Mailbox         queue;
	uint64_t        id;
	TrList          prepared;
	TrCache         cache;
	Resource        lock;

	// current state
	Consensus       consensus;

	// commited state
	_Alignas(cache_line)
	ConsensusAtomic consensus_commit;
	Consensus       consensus_commit_last;

	// pending commit state (group commit)
	bool            pending;
	Consensus       pending_consensus;
	Track*          pending_link;
	Task*           compute;
};

static inline void
track_init(Track* self)
{
	self->id           = 1;
	self->pending      = false;
	self->pending_link = NULL;
	self->compute      = NULL;
	mailbox_init(&self->queue);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	resource_init(&self->lock, "user");
	consensus_init(&self->consensus);
	consensus_atomic_init(&self->consensus_commit);
	consensus_init(&self->consensus_commit_last);
	consensus_init(&self->pending_consensus);
}

static inline void
track_free(Track* self)
{
	assert(list_empty(&self->queue.list));
	tr_list_reset(&self->prepared, &self->cache);
	tr_cache_free(&self->cache);
	resource_free(&self->lock);
}

static inline void
track_set_compute(Track* self, Task* task)
{
	self->compute = task;
}

static inline Msg*
track_read(Track* self)
{
	return mailbox_pop(&self->queue, am_self());
}

static inline Msg*
track_read_time(Track* self, int time_ms)
{
	return mailbox_pop_time(&self->queue, &am_task->clock, am_self(), time_ms);
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
	//task_send(self->compute, msg);
	track_write(self, msg);
}

hot static inline bool
track_commit(Track* self, Consensus* commit)
{
	auto changed = false;

	// commit all transactions <= abort
	auto consensus = &self->consensus;
	auto id = commit->abort;
	if (unlikely(id > consensus->abort))
	{
		tr_abort_list(&self->prepared, &self->cache, id);
		consensus->abort = id;
		changed = true;
	}

	// commit all transactions <= commit
	id = commit->commit;
	if (id > consensus->commit)
	{
		tr_commit_list(&self->prepared, &self->cache, id);
		consensus->commit = id;
		changed = true;
	}
	return changed;
}
