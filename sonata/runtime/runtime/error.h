#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Error Error;

enum
{
	ERROR_NONE,
	ERROR,
	ERROR_CONFLICT,
	ERROR_RO,
	CANCEL
};

struct Error
{
	int         code;
	char        text[256];
	int         text_len;
	const char* file;
	const char* function;
	int         line;
};

static inline void
error_init(Error* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void no_return
error_throw(Error*        self,
            ExceptionMgr* mgr,
            const char*   file,
            const char*   function, int line,
            int           code,
            const char*   fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	self->code     = code;
	self->file     = file;
	self->function = function;
	self->line     = line;
	self->text_len = vsnprintf(self->text, sizeof(self->text), fmt, args);
	va_end(args);
	exception_mgr_throw(mgr);
}
