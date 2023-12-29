#pragma once

//
// indigo
//
// SQL OLTP database
//

static inline void
guard_runner(Guard* self)
{
	if (self->function)
	{
		self->function(self->function_arg);
		self->function = NULL;
	}

	// always last unless explicit call
	auto exception = in_self()->exception_mgr.last;
	assert(exception->guard_stack == self);
	exception->guard_stack = self->prev;
}

static inline void*
unguard(Guard* self)
{
	self->function = NULL;
	return self->function_arg;
}

#define guard(self, func, func_arg) \
	Guard __attribute((cleanup(guard_runner))) self = { \
		.function = (GuardFunction)func, \
		.function_arg = func_arg, \
		.prev = ({ \
			auto exception = in_self()->exception_mgr.last; \
			auto prev = exception->guard_stack; \
			exception->guard_stack = &self; \
			prev; \
		}) \
	}
