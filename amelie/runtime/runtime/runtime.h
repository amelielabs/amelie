#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

// memory
static inline void*
am_malloc(size_t size)
{
	auto ptr = am_malloc_nothrow(size);
	if (unlikely(ptr == NULL))
		error_system();
	return ptr;
}

static inline void*
am_realloc(void* pointer, size_t size)
{
	auto ptr = am_realloc_nothrow(pointer, size);
	if (unlikely(ptr == NULL))
		error_system();
	return ptr;
}

static inline void*
palloc(size_t size)
{
	auto ptr = arena_allocate_nothrow(&am_self()->arena, size);
	if (unlikely(ptr == NULL))
		error_system();
	return ptr;
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

// string
static inline int
str_strndup_nothrow(Str* self, const void* string, int size)
{
	char* pos = am_malloc_nothrow(size + 1);
	if (unlikely(pos == NULL))
		return -1;
	memcpy(pos, string, size);
	pos[size] = 0;
	str_set_allocated(self, pos, size);
	return 0;
}

static inline void
str_strndup(Str* self, const void* string, int size)
{
	if (str_strndup_nothrow(self, string, size) == -1)
		error_system();
}

static inline void
str_strdup(Str* self, const char* string)
{
	str_strndup(self, string, strlen(string));
}

static inline void
str_copy(Str* self, Str* src)
{
	str_strndup(self, str_of(src), str_size(src));
}

// buf
static inline Buf*
buf_create(void)
{
	auto self = buf_create_nothrow(&am_task->buf_cache, 0);
	if (unlikely(self == NULL))
		error_system();
	return self;
}

static inline Buf*
buf_begin(void)
{
	auto self = buf_create();
	buf_list_add(&am_self()->buf_list, self);
	return self;
}

static inline Buf*
buf_end(Buf* self)
{
	buf_list_delete(&am_self()->buf_list, self);
	return self;
}

static inline void
buf_ref(Buf* self)
{
	self->refs++;
}

static inline void
buf_free(Buf* self)
{
	if (self->cache)
	{
		self->refs--;
		if (self->refs >= 0)
			return;
		// track double free
		assert(self->refs == -1);
		self->refs = 0;
		buf_cache_push(self->cache, self);
		return;
	}
	buf_free_memory(self);
}

static inline uint8_t**
buf_reserve(Buf* self, int size)
{
	if (unlikely(buf_reserve_nothrow(self, size) == -1))
		error_system();
	return &self->position;
}

static inline void
buf_write(Buf* self, void* data, int size)
{
	buf_reserve(self, size);
	buf_append(self, data, size);
}

static inline void*
buf_claim(Buf* self, int size)
{
	buf_reserve(self, size);
	auto pos = self->position;
	buf_advance(self, size);
	return pos;
}

always_inline hot static inline void
buf_write_str(Buf* self, Str* str)
{
	buf_write(self, str_of(str), str_size(str));
}

static inline void
buf_vprintf(Buf* self, const char* fmt, va_list args)
{
	char tmp[512];
	int  tmp_len;
	tmp_len = vsnprintf(tmp, sizeof(tmp), fmt, args);
	buf_write(self, tmp, tmp_len);
}

static inline void
buf_printf(Buf* self, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	buf_vprintf(self, fmt, args);
	va_end(args);
}

// msg
static inline Buf*
msg_begin(int id)
{
	auto self = buf_begin();
	buf_reserve(self, sizeof(Msg));
	auto msg = msg_of(self);
	msg->size = sizeof(Msg);
	msg->id   = id;
	buf_advance(self, sizeof(Msg));
	return self;
}

static inline Buf*
msg_end(Buf* self)
{
	msg_of(self)->size = buf_size(self);
	return buf_end(self);
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
	auto self = condition_create_nothrow(&am_task->condition_cache);
	if (unlikely(self == NULL))
		error_system();
	int rc;
	rc = condition_attach(self, &am_task->poller);
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
	if (unlikely(coro == NULL))
		error_system();
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
	                         am_task->log_write_arg);
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
