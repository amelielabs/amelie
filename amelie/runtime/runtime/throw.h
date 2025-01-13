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
		.prev = am_self()->exception_mgr.last, \
		.triggered = false, \
		.defer_stack = NULL \
	}; \
	am_self()->exception_mgr.last = &__exception; \
	if ( setjmp(__exception.buf) == 0 ) { \
		executable; \
	} \
	am_self()->exception_mgr.last = __exception.prev; \
	__exception.triggered; \
})

// throw
#define rethrow() \
	exception_mgr_throw(&am_self()->exception_mgr)

// error
#define error_as(code, fmt, ...) \
	report_throw(source_file, \
	             source_function, \
	             source_line, code, fmt, ## __VA_ARGS__)

#define error(fmt, ...) \
	error_as(ERROR, fmt, ## __VA_ARGS__)

#define error_system() \
	error_as(ERROR, "%s(): %s (errno: %d)", source_function, \
	         strerror(errno), errno)

// cancel
#define cancellation_point() \
({ \
	if (unlikely(am_self()->cancel)) \
		error_as(CANCEL, "cancelled"); \
})
