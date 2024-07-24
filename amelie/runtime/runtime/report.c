
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>

static inline void
report_write(const char* file,
             const char* function, int line,
             const char* prefix,
             const char* text)
{
	auto self = so_self();
	if (! so_task->log_write)
		return;

	if (self->name[0] != 0)
		so_task->log_write(so_task->log_write_arg,
		                   file,
		                   function,
		                   line,
		                   "%s %s  %s%s",
		                   so_task->name, self->name, prefix, text);
	else
		so_task->log_write(so_task->log_write_arg,
		                   file,
		                   function,
		                   line,
		                   "%s  %s%s",
		                   so_task->name, prefix, text);
}

void
report(const char* file,
       const char* function, int line,
       const char* prefix,
       const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char text[1024];
	vsnprintf(text, sizeof(text), fmt, args);
	va_end(args);

	report_write(file, function, line, prefix, text);
}

void
report_throw(const char* file,
             const char* function, int line,
             int         code,
             const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char text[1024];
	vsnprintf(text, sizeof(text), fmt, args);
	va_end(args);

	const char* prefix = "error: ";
	if (code == CANCEL)
		prefix = "";
	report_write(file, function, line, prefix, text);

	auto self = so_self();
	error_throw(&self->error,
	            &self->exception_mgr,
	            file,
	            function,
	            line,
	            code, text);
}
