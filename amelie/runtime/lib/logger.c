
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
	self->enable    = true;
	self->to_stdout = false;
	self->cli       = false;
	self->cli_lf    = true;
	self->fd        = -1;
	self->timezone  = NULL;
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
logger_set_cli(Logger* self, bool enable, bool lf)
{
	self->cli = enable;
	self->cli_lf = lf;
}

void
logger_set_to_stdout(Logger *self, bool enable)
{
	self->to_stdout = enable;
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

	if (! self->to_stdout)
		return;

	// do not print timestamp for cli
	auto data = buf->start;
	auto data_size = buf_size(buf);
	if (self->cli)
	{
		data += timestamp_offset;
		data_size -= timestamp_offset;
		if (! self->cli_lf)
			data_size--;
	}

	// write to stdout
	vfs_write(STDOUT_FILENO, data, data_size);
}
