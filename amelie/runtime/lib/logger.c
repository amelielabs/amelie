
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>

void
logger_init(Logger* self)
{
	self->enable        = true;
	self->stdout_enable = false;
	self->stdout_time   = false;
	self->stdout_lf     = false;
	self->fd            = -1;
	self->timezone      = NULL;
}

void
logger_open(Logger* self, const char* path)
{
	int rc;
	rc = vfs_open(path, O_APPEND|O_RDWR|O_CREAT, 0644);
	if (unlikely(rc == -1))
		error_system();
	self->fd = rc;
}

void
logger_close(Logger* self)
{
	if (self->fd != -1)
	{
		vfs_close(self->fd);
		self->fd = -1;
	}
}

void
logger_set_enable(Logger* self, bool enable)
{
	self->enable = enable;
}

void
logger_set_stdout(Logger *self, bool enable)
{
	self->stdout_enable = enable;
}

void
logger_set_stdout_time(Logger *self, bool enable)
{
	self->stdout_time = enable;
}

void
logger_set_stdout_lf(Logger *self, bool enable)
{
	self->stdout_lf = enable;
}

void
logger_set_timezone(Logger *self, Timezone* timezone)
{
	self->timezone = timezone;
}

void
logger_write(TaskLog* arg, const char* fmt, va_list args)
{
	Logger* self = arg->write_arg;

	auto buf = &arg->buf;
	buf_reset(buf);

	// timestamp
	buf_reserve(buf, 64);
	if (self->timezone)
	{
		timer_mgr_reset(&am_task->timer_mgr);
		auto time = time_us();
		auto rc = timestamp_get(time, self->timezone, buf_cstr(buf), 64);
		buf_advance(buf, rc);
		buf_write(buf, " ", 1);
	}

	// msg
	auto timestamp_offset = buf_size(buf);
	buf_vprintf(buf, fmt, args);
	buf_write(buf, "\n", 1);

	// write to the log file
	if (self->fd != -1)
		vfs_write(self->fd, buf->start, buf_size(buf));

	if (! self->stdout_enable)
		return;

	auto data = buf->start;
	auto data_size = buf_size(buf);

	// print timestamp
	if (! self->stdout_time)
	{
		data += timestamp_offset;
		data_size -= timestamp_offset;
	}

	// print lf
	if (! self->stdout_lf)
		data_size--;

	// write to stdout
	vfs_write(STDOUT_FILENO, data, data_size);
}
