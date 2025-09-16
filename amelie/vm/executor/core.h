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
	Ring       ring;
	atomic_u64 commit;
	atomic_u64 abort;
	int        order;
	TrList     prepared;
	TrCache    cache;
	Msg        msg_stop;
	Task*      task;
};

static inline void
core_init(Core* self, Task* task, int order)
{
	self->order  = order;
	self->commit = 0;
	self->abort  = 0;
	self->task   = task;
	ring_init(&self->ring);
	ring_prepare(&self->ring, 1024);
	msg_init(&self->msg_stop, MSG_STOP);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
}

static inline void
core_free(Core* self)
{
	assert(! ring_pending(&self->ring));
	ring_free(&self->ring);
	tr_cache_free(&self->cache);
}

static inline void
core_shutdown(Core* self)
{
	task_send(self->task, &self->msg_stop);
}

static inline void
core_abort(Core* self)
{
	atomic_u64_inc(&self->abort);
}

static inline void
core_commit(Core* self, uint64_t commit)
{
	atomic_u64_set(&self->commit, commit);
}

static inline void
core_add(Core* self, Ctr* ctr)
{
	task_send(self->task, &ctr->msg);
}
