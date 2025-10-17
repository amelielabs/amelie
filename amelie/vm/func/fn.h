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

typedef struct FnMgr FnMgr;
typedef struct Fn    Fn;

typedef enum
{
	FN_EXECUTE,
	FN_CLEANUP
} FnAction;

struct Fn
{
	int       argc;
	Value*    argv;
	Value*    result;
	Function* function;
	FnAction  action;
	Local*    local;
	void**    context;
};

no_return void fn_error(Fn*, char*, ...);
no_return void fn_error_noargs(Fn*, char*, ...);
no_return void fn_error_arg(Fn*, int, char*, ...);

static inline void
fn_expect(Fn* self, int argc)
{
	if (unlikely(self->argc != argc))
	{
		if (argc == 0)
			fn_error_arg(self, 0, "function has no arguments");
		fn_error(self, "expected %d argument%s", argc,
		         argc > 1 ? "s": "");
	}
}

static inline void
fn_expect_arg(Fn* self, int arg, Type type)
{
	if (unlikely(self->argv[arg].type != type))
		fn_error_arg(self, arg, "expected %s", type_of(type));
}

static inline void no_return
fn_unsupported(Fn* self, int arg)
{
	fn_error_arg(self, arg, "unsupported argument type");
}
