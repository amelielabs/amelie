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

always_inline static inline void
defer_runner(Defer* self)
{
	if (! self->function)
		return;
	am_self()->exception_mgr.last->defer_stack = self->prev;
	if (! self->error_only)
		self->function(self->function_arg);
}

#define defer_as(self, func, func_arg, on_error) \
	Defer __attribute((cleanup(defer_runner))) self = { \
		.function = (DeferFunction)func, \
		.function_arg = func_arg, \
		.error_only = on_error, \
		.prev = ({ \
			auto exception = am_self()->exception_mgr.last; \
			auto prev = exception->defer_stack; \
			exception->defer_stack = &self; \
			prev; \
		}) \
	}

// defer()
#define defer_auto_def(name, line, func, func_arg) \
	defer_as(name##line, func, func_arg, false)

#define defer_auto(name, line, func, func_arg) \
	defer_auto_def(name, line, func, func_arg)

#define defer(func, func_arg) \
	defer_auto(_defer_, __LINE__, func, func_arg)

// errdefer()
#define errdefer_auto_def(name, line, func, func_arg) \
	defer_as(name##line, func, func_arg, true)

#define errdefer_auto(name, line, func, func_arg) \
	errdefer_auto_def(name, line, func, func_arg)

#define errdefer(func, func_arg) \
	errdefer_auto(_defer_, __LINE__, func, func_arg)
