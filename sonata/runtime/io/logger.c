
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>

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
logger_init(Logger* self)
{
	self->enable    = true;
	self->to_stdout = true;
	self->fd        = -1;
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

static inline void
logger_writev(Logger*     self,
              const char* file,
              const char* function,
              int         line,
              const char* fmt,
              va_list     args)
{
	unused(file);
	unused(function);
	unused(line);
	if (! self->enable)
		return;

	int  len = 0;
	char buffer[1024];

	// time
	struct timeval tv;
	gettimeofday(&tv, NULL);
	len += strftime(buffer + len, sizeof(buffer) - len, "%d.%m.%y %H:%M:%S.",
	                localtime(&tv.tv_sec));
	len += snprintf(buffer + len, sizeof(buffer) - len, "%03d ",
	                (signed)tv.tv_usec / 1000);

	// message
	len += vsnprintf(buffer + len, sizeof(buffer) - len, fmt, args);
	len += snprintf(buffer + len, sizeof(buffer) - len, "\n");

	// write
	if (self->fd != -1)
		vfs_write(self->fd, buffer, len);

	if (self->to_stdout)
		vfs_write(STDOUT_FILENO, buffer, len);
}

void
logger_write(void*       self,
             const char* file,
             const char* function,
             int         line,
             const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	logger_writev(self, file, function, line, fmt, args);
	va_end(args);
}
