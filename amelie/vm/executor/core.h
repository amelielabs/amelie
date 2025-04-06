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
	Mutex    lock;
	CondVar  cond_var;
	bool     shutdown;
	int      order;
	List     list;
	int      list_count;
	uint64_t commit;
	TrList   prepared;
	TrCache  cache;
};

static inline void
core_init(Core* self, int order)
{
	self->order      = order;
	self->list_count = 0;
	self->shutdown   = false;
	self->commit     = 0;
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
	self->shutdown = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
core_add(Core* self, Ctr* ctr)
{
	mutex_lock(&self->lock);
	self->list_count++;
	list_append(&self->list, &ctr->link);
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline Ctr*
core_accept(Core* self)
{
	mutex_lock(&self->lock);
	Ctr* next = NULL;
	for (;;)
	{
		if (self->list_count > 0)
		{
			auto ref = list_pop(&self->list);
			self->list_count--;
			next = container_of(ref, Ctr, link);
			break;
		}
		if (self->shutdown)
			break;
		cond_var_wait(&self->cond_var, &self->lock);
	}
	mutex_unlock(&self->lock);
	return next;
}
