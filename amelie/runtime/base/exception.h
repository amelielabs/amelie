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

typedef struct Defer        Defer;
typedef struct Exception    Exception;
typedef struct ExceptionMgr ExceptionMgr;

typedef void (*DeferFunction)(void*);

struct Defer
{
	DeferFunction function;
	void*         function_arg;
	Defer*        prev;
	bool          error_only;
};

struct Exception
{
	Exception* prev;
	bool       triggered;
	Defer*     defer_stack;
	jmp_buf    buf;
};

struct ExceptionMgr
{
	Exception* last;
};

static inline void
exception_mgr_init(ExceptionMgr* self)
{
	self->last = NULL;
}

static inline void no_return
exception_mgr_throw(ExceptionMgr* self)
{
	auto current = self->last;
	assert(current);
	for (auto defer = current->defer_stack; defer;
	     defer = defer->prev)
	{
		defer->function(defer->function_arg);
		defer->function = NULL;
	}
	current->defer_stack = NULL;
	current->triggered = true;
	longjmp(self->last->buf, 1);
}
