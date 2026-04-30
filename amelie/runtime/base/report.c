
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

void
report(const char* file,
       const char* function, int line,
       const char* fmt, ...)
{
	unused(file);
	unused(function);
	unused(line);
	auto log = &am_task->log;
	if (task_log_active(log))
	{
		va_list args;
		va_start(args, fmt);
		task_log(log, fmt, args);
		va_end(args);
	}
}

void
report_error(const char* file,
             const char* function, int line,
             int         code,
             const char* fmt, ...)
{
	// set error context
	va_list args;
	va_start(args, fmt);
	auto error = &am_self()->error;
	error_setv(error, file, function, line, code, fmt, args);
	va_end(args);

	// do not show cancel as error
	const char* prefix = "error: ";
	bool write_log = true;
	if (unlikely(code == CANCEL))
	{
		write_log = am_self()->cancel_log;
		prefix = "";
	}

	// write log
	auto log = &am_task->log;
	if (task_log_active(log) && write_log)
		task_logf(log, "{s}{buf}", prefix, &error->text);

	// throw exception
	exception_mgr_throw(&am_self()->exception_mgr);
}

void no_return
report_panic(const char* file,
             const char* function, int line,
             const char* fmt, ...)
{
	if (am_task && task_log_active(&am_task->log))
	{
		// set error context
		va_list args;
		va_start(args, fmt);
		auto error = &am_self()->error;
		error_setv(error, file, function, line, 0, fmt, args);
		va_end(args);

		// write log
		auto log = &am_task->log;
		task_logf(log, "panic: {buf}", &error->text);
	} else
	{
		va_list args;
		va_start(args, fmt);
		char msg[512];
		formatv(msg, sizeof(msg), fmt, args);
		va_end(args);

		fprintf(stderr, "panic: %s\n", msg);
	}

	fflush(NULL);
	abort();
}
