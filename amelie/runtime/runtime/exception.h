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

typedef struct Guard        Guard;
typedef struct Exception    Exception;
typedef struct ExceptionMgr ExceptionMgr;

typedef void (*GuardFunction)(void*);

struct Guard
{
	GuardFunction function;
	void*         function_arg;
	Guard*        prev;
};

struct Exception
{
	Exception* prev;
	bool       triggered;
	Guard*     guard_stack;
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

static inline void
exception_mgr_execute_guards(Guard* guard)
{
	for (; guard; guard = guard->prev)
	{
		if (guard->function == NULL)
			continue;
		guard->function(guard->function_arg);
		guard->function = NULL;
	}
}

static inline void no_return
exception_mgr_throw(ExceptionMgr* self)
{
	auto current = self->last;
	assert(current);
	exception_mgr_execute_guards(current->guard_stack);
	current->guard_stack = NULL;
	current->triggered = true;
	longjmp(self->last->buf, 1);
}

#define exception_mgr_enter(self, exception) \
({ \
	(exception)->prev = (self)->last; \
 	(exception)->triggered = false; \
 	(exception)->guard_stack = NULL; \
	(self)->last = (exception); \
	setjmp((exception)->buf) == 0; \
})

always_inline static inline bool
exception_mgr_leave(ExceptionMgr* self, Exception* exception)
{
	self->last = exception->prev;
	return exception->triggered;
}
