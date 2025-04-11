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

typedef union  CoreEvent CoreEvent;
typedef struct Core      Core;

enum
{
	CORE_SHUTDOWN,
	CORE_RUN,
	CORE_COMMIT,
	CORE_ABORT
};

union CoreEvent
{
	Ctr*     ctr;
	uint64_t commit;
};

struct Core
{
	Mutex    lock;
	CondVar  cond_var;
	int      order;
	List     list;
	int      list_count;
	uint64_t event_commit;
	bool     event_abort;
	bool     event_shutdown;
	TrList   prepared;
	TrCache  cache;
};

static inline void
core_init(Core* self, int order)
{
	self->order          = order;
	self->list_count     = 0;
	self->event_commit   = 0;
	self->event_abort    = false;
	self->event_shutdown = false;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->list);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
}

static inline void
core_free(Core* self)
{
	assert(! self->list_count);
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
	tr_cache_free(&self->cache);
}

static inline void
core_shutdown(Core* self)
{
	mutex_lock(&self->lock);
	self->event_shutdown = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
core_abort(Core* self)
{
	mutex_lock(&self->lock);
	self->event_abort = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
core_commit(Core* self, uint64_t commit)
{
	mutex_lock(&self->lock);
	self->event_commit = commit;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
core_add(Core* self, Ctr* ctr)
{
	mutex_lock(&self->lock);
	list_append(&self->list, &ctr->link);
	self->list_count++;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

hot static inline int
core_next(Core* self, CoreEvent* event)
{
	mutex_lock(&self->lock);
	int type;
	for (;;)
	{
		// abort
		if (unlikely(self->event_abort))
		{
			type = CORE_ABORT;
			self->event_abort = false;
			break;
		}

		// commit
		if (self->event_commit > 0)
		{
			type = CORE_COMMIT;
			event->commit = self->event_commit;
			self->event_commit = 0;
			break;
		}

		// run
		if (self->list_count > 0)
		{
			auto ref = list_pop(&self->list);
			self->list_count--;
			type = CORE_RUN;
			event->ctr = container_of(ref, Ctr, link);
			break;
		}

		// shutdown
		if (unlikely(self->event_shutdown))
		{
			type = CORE_SHUTDOWN;
			break;
		}

		cond_var_wait(&self->cond_var, &self->lock);
	}
	mutex_unlock(&self->lock);
	return type;
}
