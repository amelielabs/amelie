
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
	bool log = true;

	auto self = am_self();
	if (unlikely(code == CANCEL))
	{
		prefix = "";
		log = self->cancel_log;
	}

	if (likely(am_task->log_write && log))
		am_task->log_write(am_task->log_write_arg,
		                   file,
		                   function,
		                   line,
		                   prefix, text);

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
