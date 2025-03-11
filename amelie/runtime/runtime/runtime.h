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
	return arena_allocate(&am_self->arena, size);
}

static inline int
palloc_snapshot(void)
{
	return am_self->arena.offset;
}

static inline void
palloc_truncate(int snapshot)
{
	arena_truncate(&am_self->arena, snapshot);
}

// buf
static inline Buf*
buf_create(void)
{
	return buf_mgr_create(am_self->buf_mgr, 0);
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

// msg
static inline Buf*
msg_create(int id)
{
	return msg_create_as(am_self->buf_mgr, id, 0);
}

// task
static inline void
task_create(Task*        self,
            char*        name,
            TaskFunction main,
            void*        main_arg)
{
	int rc;
	rc = task_create_nothrow(self, name, main, main_arg,
	                         am_self->main_arg_global,
	                         am_self->log_write,
	                         am_self->log_write_arg,
	                         am_self->buf_mgr);
	if (unlikely(rc == -1))
		error_system();
}

// time
static inline uint64_t
time_ms(void)
{
	return clock_time_ms(&am_self->clock);
}

static inline uint64_t
time_us(void)
{
	return clock_time_us(&am_self->clock);
}
