#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

// enter/leave
#define enter(exception) \
	exception_mgr_enter(&am_self()->exception_mgr, exception)

#define leave(exception) \
	exception_mgr_leave(&am_self()->exception_mgr, exception)

// throw
#define rethrow() \
	exception_mgr_throw(&am_self()->exception_mgr)

// errors
#define error_as(code, fmt, ...) \
	report_throw(source_file, \
	             source_function, \
	             source_line, code, fmt, ## __VA_ARGS__)

#define error(fmt, ...) \
	error_as(ERROR, fmt, ## __VA_ARGS__)

#define error_system() \
	error_as(ERROR, "%s(): %s (errno: %d)", source_function, \
	         strerror(errno), errno)

#define error_data() \
	error_as(ERROR, "data read error")

// cancel
#define cancellation_point() ({ \
	if (unlikely(am_self()->cancel)) \
		error_as(CANCEL, "cancelled"); \
})

// log
#define info(fmt, ...) \
	report(source_file, \
	       source_function, \
	       source_line, "", fmt, ## __VA_ARGS__)

#define debug(fmt, ...) \
	report(source_file, \
	       source_function, \
	       source_line, "", fmt, ## __VA_ARGS__)
