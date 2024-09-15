#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

// palloc
static inline void*
palloc(size_t size)
{
	return arena_allocate(&am_self()->arena, size);
}

static inline int
palloc_snapshot(void)
{
	return am_self()->arena.offset;
}

static inline void
palloc_truncate(int snapshot)
{
	arena_truncate(&am_self()->arena, snapshot);
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
		self->refs--;
		if (self->refs >= 0)
			return;
		assert(self->refs == -1);
		self->refs = 0;
		buf_cache_push(self->cache, self);
		return;
	}
	buf_free_memory(self);
}

// msg
static inline Buf*
msg_create(int id)
{
	return msg_create_as(am_task->buf_mgr, id, 0);
}

// event
hot static inline bool
event_wait(Event* self, int time_ms)
{
	cancellation_point();
	bool timedout = wait_event(self, &am_task->timer_mgr, am_self(), time_ms);
	cancellation_point();
	return timedout;
}

// condition
static inline Condition*
condition_create(void)
{
	auto self = condition_cache_create(&am_task->condition_cache);
	auto rc   = condition_attach(self, &am_task->poller);
	if (unlikely(rc == -1))
	{
		condition_cache_push(&am_task->condition_cache, self);
		error_system();
	}
	return self;
}

static inline void
condition_free(Condition* self)
{
	condition_cache_push(&am_task->condition_cache, self);
}

hot static inline bool
condition_wait(Condition* self, int time_ms)
{
	return event_wait(&self->event, time_ms);
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
	wait_event(&coro->on_exit, &am_task->timer_mgr, self, -1);
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
	wait_event(&coro->on_exit, &am_task->timer_mgr, self, -1);
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

// task
static inline void
task_create(Task*        self,
            char*        name,
            MainFunction main,
            void*        main_arg)
{
	int rc;
	rc = task_create_nothrow(self, name, main, main_arg,
	                         am_task->main_arg_global,
	                         am_task->log_write,
	                         am_task->log_write_arg,
	                         am_task->buf_mgr);
	if (unlikely(rc == -1))
		error_system();
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
