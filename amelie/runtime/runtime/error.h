#pragma once

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

typedef struct Error Error;

enum
{
	ERROR_NONE,
	ERROR,
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

static inline void
error_set(Error*      self,
          const char* file,
          const char* function,
          int         line,
          int         code,
          const char* text)
{
	self->code     = code;
	self->file     = file;
	self->function = function;
	self->line     = line;
	self->text_len = snprintf(self->text, sizeof(self->text), "%s", text);
}

static inline void no_return
error_throw(Error*        self,
            ExceptionMgr* mgr,
            const char*   file,
            const char*   function, int line,
            int           code,
            const char*   text)
{
	error_set(self, file, function, line, code, text);
	exception_mgr_throw(mgr);
}
