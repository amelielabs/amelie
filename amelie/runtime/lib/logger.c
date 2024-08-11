
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

void
logger_init(Logger* self)
{
	self->enable    = true;
	self->to_stdout = false;
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
             const char* fmt, ...)
{
	Logger* self = arg;

	unused(file);
	unused(function);
	unused(line);
	if (! self->enable)
		return;

	int  buf_len = 0;
	char buf[1024];

	va_list args;
	va_start(args, fmt);

	// timestamp
	if (self->timezone)
	{
		auto time = time_us();
		buf_len = timestamp_write(time, self->timezone, buf, sizeof(buf));
		buf[buf_len++] = ' ';
	}

	// message
	buf_len += vsnprintf(buf + buf_len, sizeof(buf) - buf_len, fmt, args);
	buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len, "\n");
	va_end(args);

	// write
	if (self->fd != -1)
		vfs_write(self->fd, buf, buf_len);

	if (self->to_stdout)
		vfs_write(STDOUT_FILENO, buf, buf_len);
}
