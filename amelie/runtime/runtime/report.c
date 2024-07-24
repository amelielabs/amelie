
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>

static inline void
report_write(const char* file,
             const char* function, int line,
             const char* prefix,
             const char* text)
{
	auto self = am_self();
	if (! am_task->log_write)
		return;

	if (self->name[0] != 0)
		am_task->log_write(am_task->log_write_arg,
		                   file,
		                   function,
		                   line,
		                   "%s %s  %s%s",
		                   am_task->name, self->name, prefix, text);
	else
		am_task->log_write(am_task->log_write_arg,
		                   file,
		                   function,
		                   line,
		                   "%s  %s%s",
		                   am_task->name, prefix, text);
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

	auto self = am_self();
	error_throw(&self->error,
	            &self->exception_mgr,
	            file,
	            function,
	            line,
	            code, text);
}
