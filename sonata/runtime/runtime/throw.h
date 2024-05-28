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

// errors
#define error_as(code, fmt, ...) \
	report(source_file, \
	       source_function, \
	       source_line, code, "error: ", true, fmt, ## __VA_ARGS__)

#define error(fmt, ...) \
	error_as(ERROR, fmt, ## __VA_ARGS__)

#define error_system() \
	error_as(ERROR, "%s(): %s (errno: %d)", source_function, \
	         strerror(errno), errno)

#define error_data() \
	error_as(ERROR, "data read error")

// cancel
#define cancellation_point() ({ \
	if (unlikely(so_self()->cancel)) \
		error_as(CANCEL, "cancelled"); \
})

// log
#define log(fmt, ...) \
	report(source_file, \
	       source_function, \
	       source_line, 0, "", false, fmt, ## __VA_ARGS__)
