
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

#include <amelie_runtime.h>
#include <amelie_io.h>
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
logger_write(void*       arg,
             const char* file,
             const char* function,
             int         line,
             const char* prefix,
             const char* text)
{
	Logger* self = arg;

	unused(file);
	unused(function);
	unused(line);

	if (! self->enable)
		return;

	int  buf_len = 0;
	char buf[1024];

	if (self->fd == -1 && self->cli && self->to_stdout)
		goto print_stdout;

	// timestamp
	if (self->timezone)
	{
		timer_mgr_reset(&am_task->timer_mgr);
		auto time = time_us();
		buf_len = timestamp_get(time, self->timezone, buf, sizeof(buf));
		buf[buf_len++] = ' ';
	}

	// message
	buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len, " %s%s\n",
	                    prefix, text);

	// write to the log file
	if (self->fd != -1)
		vfs_write(self->fd, buf, buf_len);

print_stdout:
	// cli print (simplified output)
	if (self->cli)
		buf_len = snprintf(buf, sizeof(buf), "%s%s%s", prefix, text,
		                   self->cli_lf ? "\n" : "");

	// print
	if (self->to_stdout)
		vfs_write(STDOUT_FILENO, buf, buf_len);
}
