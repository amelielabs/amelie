#pragma once

//
// indigo
//
// SQL OLTP database
//

// try-catch
#define try(exception) \
	exception_mgr_try(&in_self()->exception_mgr, exception)

#define catch(exception) \
	exception_mgr_catch(&in_self()->exception_mgr, exception)

// throw
#define rethrow() \
	exception_mgr_throw(&in_self()->exception_mgr)

#define throw(code, fmt, ...) \
	error_throw(&in_self()->error, \
	            &in_self()->exception_mgr, \
	            source_file, \
	            source_function, \
	            source_line, \
	            code, fmt, ## __VA_ARGS__)

// errors
#define error(fmt, ...) \
	throw(ERROR, fmt, ## __VA_ARGS__)

#define error_system() \
	throw(ERROR, "%s(): %s (errno: %d)", source_function, \
	      strerror(errno), errno)

#define error_data() \
	throw(ERROR, "data read error")

// cancel
#define cancellation_point() ({ \
	if (unlikely(in_self()->cancel)) \
		throw(CANCEL, "cancelled"); \
})
