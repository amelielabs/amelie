
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
	if (! am_self->log_write)
		return;

	va_list args;
	va_start(args, fmt);
	char text[1024];
	vsnprintf(text, sizeof(text), fmt, args);
	va_end(args);

	am_self->log_write(am_self->log_write_arg,
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

	if (unlikely(code == CANCEL))
	{
		prefix = "";
		log = am_self->log_cancel;
	}

	if (likely(am_self->log_write && log))
		am_self->log_write(am_self->log_write_arg,
		                   file,
		                   function,
		                   line,
		                   prefix, text);

	error_throw(&am_self->error,
	            &am_self->exception_mgr,
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

	if (am_self && am_self->log_write)
	{
		const char* prefix = "panic: ";
		if (am_self->log_write)
			am_self->log_write(am_self->log_write_arg,
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
