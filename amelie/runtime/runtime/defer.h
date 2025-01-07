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

always_inline static inline Defer*
defer_pop(void)
{
	auto exception = am_self()->exception_mgr.last;
	auto self = exception->defer_stack;
	exception->defer_stack = self->prev;
	return self;
}

always_inline static inline void
defer_runner(Defer* self)
{
	if (! self->function)
		return;
	defer_pop();
	self->function(self->function_arg);
	self->function = NULL;
}

always_inline static inline void*
undefer(void)
{
	auto self = defer_pop();
	self->function = NULL;
	return self->function_arg;
}

#define defer_as(self, func, func_arg) \
	Defer __attribute((cleanup(defer_runner))) self = { \
		.function = (DeferFunction)func, \
		.function_arg = func_arg, \
		.prev = ({ \
			auto exception = am_self()->exception_mgr.last; \
			auto prev = exception->defer_stack; \
			exception->defer_stack = &self; \
			prev; \
		}) \
	}

#define defer_auto_def(name, line, func, func_arg) \
	defer_as(name##line, func, func_arg)

#define defer_auto(name, line, func, func_arg) \
	defer_auto_def(name, line, func, func_arg)

#define defer(func, func_arg) \
	defer_auto(_defer_, __LINE__, func, func_arg)
