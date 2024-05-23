#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

// enter/leave
#define enter(exception) \
	exception_mgr_enter(&so_self()->exception_mgr, exception)

#define leave(exception) \
	exception_mgr_leave(&so_self()->exception_mgr, exception)

// throw
#define rethrow() \
	exception_mgr_throw(&so_self()->exception_mgr)

#define error_throw(code, fmt, ...) \
	error_throw_as(&so_self()->error, \
	               &so_self()->exception_mgr, \
	               source_file, \
	               source_function, \
	               source_line, \
	               code, fmt, ## __VA_ARGS__)

// errors
#define error(fmt, ...) \
	error_throw(ERROR, fmt, ## __VA_ARGS__)

#define error_system() \
	error_throw(ERROR, "%s(): %s (errno: %d)", source_function, \
	            strerror(errno), errno)

#define error_data() \
	error_throw(ERROR, "data read error")

// cancel
#define cancellation_point() ({ \
	if (unlikely(so_self()->cancel)) \
		error_throw(CANCEL, "cancelled"); \
})
