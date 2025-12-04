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
	int   code;
	char* file;
	char* function;
	int   line;
	Buf   text;
};

static inline void
error_init(Error* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
error_free(Error* self)
{
	buf_free_memory(&self->text);
}

static inline void
error_reset(Error* self)
{
	self->code     = 0;
	self->file     = NULL;
	self->function = NULL;
	self->line     = 0;
	buf_reset(&self->text);
}

static inline void
error_setv(Error*      self,
           const char* file,
           const char* function,
           int         line,
           int         code,
           const char* fmt,
           va_list     fmt_args)
{
	self->code     = code;
	self->file     = (char*)file;
	self->function = (char*)function;
	self->line     = line;
	buf_reset(&self->text);
	buf_vprintf(&self->text, fmt, fmt_args);
}

static inline void format_validate(6, 7)
error_set(Error*      self,
          const char* file,
          const char* function,
          int         line,
          int         code,
          const char* fmt,
          ...)
{
	va_list args;
	va_start(args, fmt);
	error_setv(self, file, function, line, code, fmt, args);
	va_end(args);
}

static inline void
error_copy(Error* self, Error* from)
{
	self->code     = from->code;
	self->file     = from->file;
	self->function = from->function;
	self->line     = from->line;
	buf_reset(&self->text);
	buf_write_buf(&self->text, &from->text);
}
