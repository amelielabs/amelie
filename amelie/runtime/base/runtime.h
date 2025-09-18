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

// palloc
static inline void*
palloc(size_t size)
{
	return arena_allocate(&am_self()->arena, size);
}

static inline void
palloc_reset(void)
{
	arena_reset(&am_self()->arena);
}

// buf
static inline Buf*
buf_create(void)
{
	return buf_mgr_create(am_task->buf_mgr, 0);
}

static inline void
buf_free(Buf* self)
{
	if (self->cache)
	{
		if (atomic_u32_dec(&self->refs) == 0)
		{
			self->refs = 0;
			buf_cache_push(self->cache, self);
		}
		return;
	}
	buf_free_memory(self);
}

// event
static inline void
event_attach(Event* self)
{
	bus_attach(&am_task->bus, self);
}

hot static inline void
event_signal(Event* self)
{
	if (self->bus)
		bus_send(self->bus, &self->ipc);
	else
		event_signal_local(self);
}

hot static inline bool
event_wait(Event* self, int time_ms)
{
	cancellation_point();
	bool timedout = false;
	if (time_ms >= 0)
		timedout = wait_event_time(self, &am_task->timer_mgr, am_self(), time_ms);
	else
		wait_event(self, am_self());
	cancellation_point();
	return timedout;
}

// coroutine
static inline uint64_t
coroutine_create(MainFunction function, void* arg)
{
	auto coro =
		coroutine_mgr_create(&am_task->coroutine_mgr, task_coroutine_main,
		                     function, arg);
	return coro->id;
}

static inline void
coroutine_wait(uint64_t id)
{
	auto coro = coroutine_mgr_find(&am_task->coroutine_mgr, id);
	if (coro == NULL)
		return;
	auto self = am_self();
	coroutine_cancel_pause(self);
	wait_event(&coro->on_exit, self);
	coroutine_cancel_resume(self);
}

static inline void
coroutine_kill_nowait(uint64_t id)
{
	auto coro = coroutine_mgr_find(&am_task->coroutine_mgr, id);
	if (coro == NULL)
		return;
	coroutine_cancel(coro);
}

static inline void
coroutine_kill(uint64_t id)
{
	auto coro = coroutine_mgr_find(&am_task->coroutine_mgr, id);
	if (coro == NULL)
		return;
	coroutine_cancel(coro);
	auto self = am_self();
	coroutine_cancel_pause(self);
	wait_event(&coro->on_exit, self);
	coroutine_cancel_resume(self);
}

static inline void
coroutine_sleep(int time_ms)
{
	Event on_timer;
	event_init(&on_timer);
	event_wait(&on_timer, time_ms);
}

static inline void
coroutine_yield(void)
{
	Event on_timer;
	event_init(&on_timer);
	event_wait(&on_timer, 0);
}

static inline void
cancel_pause(void)
{
	coroutine_cancel_pause(am_self());
}

static inline void
cancel_resume(void)
{
	coroutine_cancel_resume(am_self());
}

// task
static inline void
task_create(Task*        self,
            char*        name,
            MainFunction main,
            void*        main_arg)
{
	int rc;
	rc = task_create_nothrow(self, name, main, main_arg,
	                         am_runtime,
	                         am_share,
	                         am_task->log_write,
	                         am_task->log_write_arg,
	                         am_task->buf_mgr);
	if (unlikely(rc == -1))
		error_system();
}

static inline Msg*
task_recv(void)
{
	return mailbox_pop(&am_task->bus.mailbox, am_self());
}

static inline Msg*
task_recv_try(void)
{
	return mailbox_pop_nowait(&am_task->bus.mailbox);
}

static inline void
task_send(Task* dest, Msg* msg)
{
	bus_send(&dest->bus, &msg->ipc);
}

// time
static inline uint64_t
time_ms(void)
{
	return timer_mgr_time_ms(&am_task->timer_mgr);
}

static inline uint64_t
time_us(void)
{
	return timer_mgr_time_us(&am_task->timer_mgr);
}
