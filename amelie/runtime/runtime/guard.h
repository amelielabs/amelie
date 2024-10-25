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

always_inline static inline Guard*
guard_pop(void)
{
	auto exception = am_self()->exception_mgr.last;
	auto self = exception->guard_stack;
	exception->guard_stack = self->prev;
	return self;
}

always_inline static inline void
guard_runner(Guard* self)
{
	if (! self->function)
		return;
	guard_pop();
	self->function(self->function_arg);
	self->function = NULL;
}

always_inline static inline void*
unguard(void)
{
	auto self = guard_pop();
	self->function = NULL;
	return self->function_arg;
}

#define guard_as(self, func, func_arg) \
	Guard __attribute((cleanup(guard_runner))) self = { \
		.function = (GuardFunction)func, \
		.function_arg = func_arg, \
		.prev = ({ \
			auto exception = am_self()->exception_mgr.last; \
			auto prev = exception->guard_stack; \
			exception->guard_stack = &self; \
			prev; \
		}) \
	}

#define guard_auto_def(name, line, func, func_arg) \
	guard_as(name##line, func, func_arg)

#define guard_auto(name, line, func, func_arg) \
	guard_auto_def(name, line, func, func_arg)

#define guard(func, func_arg) \
	guard_auto(_guard_, __LINE__, func, func_arg)
