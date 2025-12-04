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

void format_validate(4, 5)
report(const char* file,
       const char* function, int line,
       const char* fmt, ...);

void no_return format_validate(5, 6)
report_error(const char* file,
             const char* function, int line,
             int         code,
             const char* fmt, ...);

void no_return format_validate(4, 5)
report_panic(const char* file,
             const char* function, int line,
             const char* fmt, ...);

// log
#define info(fmt, ...) \
	report(source_file, \
	       source_function, \
	       source_line, fmt, ## __VA_ARGS__)

// panic
#define panic(fmt, ...) \
	report_panic(source_file, \
	             source_function, \
	             source_line, fmt, ## __VA_ARGS__)

#define oom() \
	panic("memory allocation failed")

// error
#define error_as(code, fmt, ...) \
	report_error(source_file, \
	             source_function, \
	             source_line, code, fmt, ## __VA_ARGS__)

#define error(fmt, ...) \
	error_as(ERROR, fmt, ## __VA_ARGS__)

#define error_system() \
	error_as(ERROR, "%s(): %s (errno: %d)", source_function, \
	         strerror(errno), errno)
