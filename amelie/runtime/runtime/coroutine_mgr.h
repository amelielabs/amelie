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

typedef struct CoroutineMgr CoroutineMgr;

struct CoroutineMgr
{
	Coroutine* current;
	List       list_free;
	List       list_ready;
	List       list;
	int        count_free;
	int        count_ready;
	int        count;
	int        stack_size;
	uint64_t   seq;
	Event      on_exit;
	Coroutine  main;
};

static inline void
coroutine_mgr_init(CoroutineMgr* self, int stack_size)
{
	self->seq         = 0;
	self->count       = 0;
	self->count_ready = 0;
	self->count_free  = 0;
	self->stack_size  = stack_size;
	list_init(&self->list);
	list_init(&self->list_ready);
	list_init(&self->list_free);
	event_init(&self->on_exit);
	memset(&self->main, 0, sizeof(self->main));
	self->current = &self->main;
}

static inline void
coroutine_mgr_free(CoroutineMgr* self)
{
	list_foreach_safe(&self->list_free)
	{
		auto coro = list_at(Coroutine, link);
		coroutine_free(coro);
	}
}

hot static inline void
coroutine_mgr_add(CoroutineMgr* self, Coroutine* coro)
{
	list_append(&self->list, &coro->link);
	self->count++;
}

hot static inline void
coroutine_mgr_del(CoroutineMgr* self, Coroutine* coro)
{
	assert(self->count > 0);
	list_unlink(&coro->link);
	self->count--;
}

hot static inline void
coroutine_mgr_add_ready(CoroutineMgr* self, Coroutine* coro)
{
	list_append(&self->list_ready, &coro->link_ready);
	self->count_ready++;
}

static inline void
coroutine_mgr_add_free(CoroutineMgr* self, Coroutine* coro)
{
	list_append(&self->list_free, &coro->link);
	self->count_free++;
}

hot static inline Coroutine*
coroutine_mgr_find(CoroutineMgr* self, uint64_t id)
{
	list_foreach(&self->list) {
		auto coro = list_at(Coroutine, link);
		if (coro->id == id)
			return coro;
	}
	return NULL;
}

hot static inline void
coroutine_mgr_switch(CoroutineMgr* self)
{
	auto current = self->current;
	self->current = &self->main;
	context_swap(&current->context, &self->main.context);
}

hot static inline void
coroutine_mgr_scheduler(CoroutineMgr* self)
{
	while (self->count_ready > 0)
	{
		auto next = list_pop(&self->list_ready);
		self->count_ready--;
		auto current = container_of(next, Coroutine, link_ready);
		list_init(&current->link_ready);
		self->current = current;
		context_swap(&self->main.context, &current->context);
	}
}

static inline Coroutine*
coroutine_mgr_create(CoroutineMgr* self,
                     MainFunction  main_runner,
                     MainFunction  main,
                     void*         main_arg)
{
	Coroutine* coro;
	if (likely(self->count_free > 0))
	{
		auto next = list_pop(&self->list_free);
		self->count_free--;
		coro = container_of(next, Coroutine, link);
		coro->cancel            = false;
		coro->cancel_pause      = 0;
		coro->cancel_pause_recv = 0;
		list_init(&coro->link_ready);
		list_init(&coro->link);
		event_init(&coro->on_exit);
		arena_reset(&coro->arena);
	} else
	{
		coro = am_malloc(sizeof(Coroutine));
		coroutine_init(coro, self);
		context_stack_allocate(&coro->stack, self->stack_size);
	}
	coro->id       = self->seq++;
	coro->main     = main;
	coro->main_arg = main_arg;

	context_create(&coro->context, &coro->stack, main_runner, coro);
	coroutine_mgr_add(self, coro);
	coroutine_mgr_add_ready(self, coro);
	return coro;
}

hot static inline void
coroutine_suspend(Coroutine* coro)
{
	// only current coroutine
	CoroutineMgr* self = coro->mgr;
	assert(self->current == coro);
	coroutine_mgr_switch(self);
}

hot static inline void
coroutine_resume(Coroutine* coro)
{
	if (! list_empty(&coro->link_ready))
		return;
	CoroutineMgr* self = coro->mgr;
	coroutine_mgr_add_ready(self, coro);
}

static inline void
coroutine_cancel(Coroutine* coro)
{
	if (coro->cancel_pause > 0)
	{
		coro->cancel_pause_recv++;
		return;
	}
	coro->cancel_pause_recv = 0;
	coro->cancel = true;
	coroutine_resume(coro);
}
