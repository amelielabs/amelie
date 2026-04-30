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

typedef struct TaskLogIf TaskLogIf;
typedef struct TaskLog   TaskLog;

typedef void (*TaskLogWrite)(TaskLog*, const char*, va_list);

struct TaskLog
{
	TaskLogWrite write;
	void*        write_arg;
	Buf          buf;
};

static inline void
task_log_init(TaskLog* self)
{
	self->write = NULL;
	self->write_arg = NULL;
	buf_init(&self->buf);
}

static inline void
task_log_free(TaskLog* self)
{
	buf_free_memory(&self->buf);
}

static inline bool
task_log_active(TaskLog* self)
{
	return self->write != NULL;
}

static inline void
task_log_set(TaskLog* self, TaskLogWrite write, void* write_arg)
{
	self->write = write;
	self->write_arg = write_arg;
}

static inline void
task_log(TaskLog* self, const char* fmt, va_list args)
{
	self->write(self, fmt, args);
}

static inline void
task_logf(TaskLog* self, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	self->write(self, fmt, args);
	va_end(args);
}
