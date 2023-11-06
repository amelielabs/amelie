#pragma once

//
// monotone
//
// SQL OLTP database
//

// memory
static inline void*
mn_malloc(size_t size)
{
	auto ptr = mn_malloc_nothrow(size);
	if (unlikely(ptr == NULL))
		error_system();
	return ptr;
}

static inline void*
mn_realloc(void* pointer, size_t size)
{
	auto ptr = mn_realloc_nothrow(pointer, size);
	if (unlikely(ptr == NULL))
		error_system();
	return ptr;
}

static inline void*
palloc(size_t size)
{
	auto ptr = arena_allocate_nothrow(&mn_self()->arena, size);
	if (unlikely(ptr == NULL))
		error_system();
	return ptr;
}

static inline int
palloc_snapshot(void)
{
	return mn_self()->arena.offset;
}

static inline void
palloc_truncate(int snapshot)
{
	arena_truncate(&mn_self()->arena, snapshot);
}

// string
static inline int
str_strndup_nothrow(Str* str, const void* string, int size)
{
	char* pos = mn_malloc_nothrow(size + 1);
	if (unlikely(pos == NULL))
		return -1;
	memcpy(pos, string, size);
	pos[size] = 0;
	str_set_allocated(str, pos, size);
	return 0;
}

static inline void
str_strndup(Str* str, const void* string, int size)
{
	if (str_strndup_nothrow(str, string, size) == -1)
		error_system();
}

static inline void
str_strdup(Str* str, const char* string)
{
	str_strndup(str, string, strlen(string));
}

static inline void
str_copy(Str* str, Str* src)
{
	str_strndup(str, str_of(src), str_size(src));
}

// buf
static inline Buf*
buf_pin(Buf* buf)
{
	assert(buf->allocated);
	buf_pool_add(&mn_self()->buf_pool, buf);
	return buf;
}

static inline Buf*
buf_unpin(Buf* buf)
{
	assert(buf->allocated);
	buf_pool_detach(buf);
	return buf;
}
	
static inline Buf*
buf_create(int size)
{
	auto buf = buf_create_nothrow(mn_task->buf_cache, size);
	if (unlikely(buf == NULL))
		error_system();
	buf_pin(buf);
	return buf;
}

static inline void
buf_ref(Buf* buf)
{
	buf->refs++;
}

static inline void
buf_free(Buf* buf)
{
	if (buf->allocated)
	{
		buf->refs--;
		if (buf->refs >= 0)
			return;
		// track double free
		assert(buf->refs == -1);
		buf->refs = 0;
		buf_unpin(buf);
		buf_cache_push(mn_task->buf_cache, buf);
		return;
	}
	buf_free_memory(buf);
}

static inline uint8_t**
buf_reserve(Buf* buf, int size)
{
	if (unlikely(buf_reserve_nothrow(buf, size) == -1))
		error_system();
	return &buf->position;
}

static inline void
buf_write(Buf* buf, void* data, int size)
{
	buf_reserve(buf, size);
	buf_append(buf, data, size);
}

always_inline hot static inline void
buf_write_str(Buf* buf, Str* str)
{
	buf_write(buf, str_of(str), str_size(str));
}

static inline void
buf_vprintf(Buf* buf, const char* fmt, va_list args)
{
	char tmp[512];
	int  tmp_len;
	tmp_len = vsnprintf(tmp, sizeof(tmp), fmt, args);
	buf_write(buf, tmp, tmp_len);
}

static inline void
buf_printf(Buf* buf, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	buf_vprintf(buf, fmt, args);
	va_end(args);
}

// msg
static inline Buf*
msg_create(int id)
{
	auto buf = msg_create_nothrow(mn_task->buf_cache, id, 0);
	if (unlikely(buf == NULL))
		error_system();
	buf_pin(buf);
	return buf;
}

// event
hot static inline bool
event_wait(Event* event, int time_ms)
{
	cancellation_point();
	bool timedout = wait_event(event, &mn_task->timer_mgr, mn_self(), time_ms);
	cancellation_point();
	return timedout;
}

// condition
static inline Condition*
condition_create(void)
{
	auto cond = condition_create_nothrow(&mn_task->condition_cache);
	if (unlikely(cond == NULL))
		error_system();
	int rc;
	rc = condition_attach(cond, &mn_task->poller);
	if (unlikely(rc == -1))
	{
		condition_cache_push(&mn_task->condition_cache, cond);
		error_system();
	}
	return cond;
}

static inline void
condition_free(Condition* cond)
{
	condition_cache_push(&mn_task->condition_cache, cond);
}

hot static inline bool
condition_wait(Condition* cond, int time_ms)
{
	return event_wait(&cond->event, time_ms);
}

// coroutine
static inline uint64_t
coroutine_create(MainFunction function, void* arg)
{
	auto coro =
		coroutine_mgr_create(&mn_task->coroutine_mgr, task_coroutine_main,
		                     function, arg);
	if (unlikely(coro == NULL))
		error_system();
	return coro->id;
}

static inline void
coroutine_wait(uint64_t id)
{
	auto coro = coroutine_mgr_find(&mn_task->coroutine_mgr, id);
	if (coro == NULL)
		return;
	auto self = mn_self();
	coroutine_cancel_pause(self);
	wait_event(&coro->on_exit, &mn_task->timer_mgr, self, -1);
	coroutine_cancel_resume(self);
}

static inline void
coroutine_kill_nowait(uint64_t id)
{
	auto coro = coroutine_mgr_find(&mn_task->coroutine_mgr, id);
	if (coro == NULL)
		return;
	coroutine_cancel(coro);
}

static inline void
coroutine_kill(uint64_t id)
{
	auto coro = coroutine_mgr_find(&mn_task->coroutine_mgr, id);
	if (coro == NULL)
		return;
	coroutine_cancel(coro);
	auto self = mn_self();
	coroutine_cancel_pause(self);
	wait_event(&coro->on_exit, &mn_task->timer_mgr, self, -1);
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
	                         mn_task->main_arg_global,
	                         mn_task->log,
	                         mn_task->log_arg);
	if (unlikely(rc == -1))
		error_system();
}

// time
static inline uint64_t
time_ms(void)
{
	return timer_mgr_time_ms(&mn_task->timer_mgr);
}

// log
static inline void
log_at(const char* file,
       const char* function, int line,
       const char* prefix,
       const char* fmt, ...)
{
	if (mn_task->log == NULL)
		return;

	va_list args;
	va_start(args, fmt);
	char text[1024];
	vsnprintf(text, sizeof(text), fmt, args);
	va_end(args);

	auto self = mn_self();
	if (self->name[0] != 0)
		mn_task->log(mn_task->log_arg,
		             file,
		             function,
		             line,
		             "%s %s  %s%s",
		             mn_task->name, self->name, prefix, text);
	else
		mn_task->log(mn_task->log_arg,
		             file,
		             function,
		             line,
		             "%s  %s%s",
		             mn_task->name, prefix, text);
}

#define log(fmt, ...) \
	log_at(source_file, \
	       source_function, \
	       source_line, "", fmt, ## __VA_ARGS__)
