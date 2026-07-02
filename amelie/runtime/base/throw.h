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

// error catch
#define error_catch(executable) \
({ \
	Exception __exception = { \
		.prev = am_self()->exceptions.last, \
		.triggered = false, \
		.defer_stack = NULL \
	}; \
	am_self()->exceptions.last = &__exception; \
	if ( setjmp(__exception.buf) == 0 ) { \
		executable; \
	} \
	am_self()->exceptions.last = __exception.prev; \
	__exception.triggered; \
})

no_return static inline void
rethrow(void)
{
	exceptions_throw(&am_self()->exceptions);
}

static inline void
cancellation_point(void)
{
	if (unlikely(am_self()->cancel))
		error_as(CANCEL, "cancelled");
}
