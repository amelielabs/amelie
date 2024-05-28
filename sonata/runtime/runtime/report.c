
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>

void
report(const char* file,
       const char* function, int line,
       int         code,
       const char* prefix,
       bool        error,
       const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char text[1024];
	vsnprintf(text, sizeof(text), fmt, args);
	va_end(args);

	if (code == CANCEL)
		prefix = "";

	auto self = so_self();
	if (so_task->log_write)
	{
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

	if (error)
	{
		error_throw(&self->error,
		            &self->exception_mgr,
		             file,
		             function,
		             line,
	                 code, text);
	}
}
