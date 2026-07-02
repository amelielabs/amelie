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
	Function* function;
	CallType  type;
	Local*    local;
	void**    context;
};

no_return void call_error(Call*, char*, ...);
no_return void call_error_noargs(Call*, char*, ...);
no_return void call_error_at(Call*, int, char*, ...);

static inline void
call_expect(Call* self, int argc)
{
	if (unlikely(self->argc != argc))
	{
		if (argc == 0)
			call_error_at(self, 0, "function has no arguments");
		call_error(self, "expected {d} argument{s}", argc,
		           argc > 1 ? "s": "");
	}
}

static inline Value*
call_arg(Call* self, int arg, Type type)
{
	if (unlikely(self->argv[arg].type != type))
		call_error_at(self, arg, "expected {s}", type_of(type));
	return &self->argv[arg];
}

static inline void no_return
call_unsupported(Call* self, int arg)
{
	call_error_at(self, arg, "unsupported argument type");
}
