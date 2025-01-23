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

typedef struct Call Call;
typedef struct Vm   Vm;

typedef enum
{
	CALL_EXECUTE,
	CALL_CLEANUP
} CallType;

struct Call
{
	int       argc;
	Value*    argv;
	Value*    result;
	Vm*       vm;
	CallType  type;
	Function* function;
	void**    context;
};

static inline void no_return
call_error(Call* self, char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);
	buf_printf(buf, "%.*s(", str_size(&self->function->name),
	           str_of(&self->function->name));
	for (auto i = 0; i < self->argc; i++)
	{
		if (i > 0)
			buf_printf(buf, ", ");
		buf_printf(buf, "%s", type_of(self->argv[i].type));
	}
	buf_printf(buf, ")");

	va_list args;
	char msg[256];
	va_start(args, fmt);
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);
	error("%.*s ⟵ %s", buf_size(buf), buf->start, msg);
}

static inline void no_return
call_error_arg(Call* self, int arg, char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);
	buf_printf(buf, "%.*s(", str_size(&self->function->name),
	           str_of(&self->function->name));
	for (auto i = 0; i < self->argc; i++)
	{
		if (i > 0)
			buf_printf(buf, ", ");
		if (i == arg) {
			buf_printf(buf, "❰%s❱", type_of(self->argv[i].type));
			break;
		}
		buf_printf(buf, "%s", type_of(self->argv[i].type));
	}

	va_list args;
	char msg[256];
	va_start(args, fmt);
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);
	error("%.*s ⟵ %s", buf_size(buf), buf->start, msg);
}

static inline void
call_expect(Call* self, int argc)
{
	if (unlikely(self->argc != argc))
	{
		if (argc == 0)
			call_error_arg(self, 0, "function has no arguments");
		else
			call_error(self, "expected %d argument%s", argc,
			           argc > 1 ? "s": "");
	}
}

static inline void
call_expect_arg(Call* self, int arg, Type type)
{
	if (unlikely(self->argv[arg].type != type))
		call_error_arg(self, arg, "expected %s", type_of(type));
}

static inline void no_return
call_unsupported(Call* self, int arg)
{
	call_error_arg(self, arg, "unsupported argument type");
}
