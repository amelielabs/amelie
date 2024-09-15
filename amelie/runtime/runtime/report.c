
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>

void
report(const char* file,
       const char* function, int line,
       const char* prefix,
       const char* fmt, ...)
{
	if (! am_task->log_write)
		return;

	va_list args;
	va_start(args, fmt);
	char text[1024];
	vsnprintf(text, sizeof(text), fmt, args);
	va_end(args);

	am_task->log_write(am_task->log_write_arg,
	                   file,
	                   function,
	                   line,
	                   prefix, text);
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
	if (am_task->log_write)
		am_task->log_write(am_task->log_write_arg,
		                   file,
		                   function,
		                   line,
		                   prefix, text);

	auto self = am_self();
	error_throw(&self->error,
	            &self->exception_mgr,
	            file,
	            function,
	            line,
	            code, text);
}

void no_return
report_panic(const char* file,
             const char* function, int line,
             const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char text[1024];
	vsnprintf(text, sizeof(text), fmt, args);
	va_end(args);

	if (am_task && am_task->log_write)
	{
		const char* prefix = "panic: ";
		if (am_task->log_write)
			am_task->log_write(am_task->log_write_arg,
			                   file,
			                   function,
			                   line,
			                   prefix, text);
	} else
	{
		fprintf(stderr, "panic: %s\n", text);
	}

	fflush(NULL);
	abort();
}
