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

void
report(const char* file,
       const char* function, int line,
       const char* prefix,
       const char* fmt, ...);

void no_return
report_throw(const char* file,
             const char* function, int line,
             int         code,
             const char* fmt, ...);

void no_return
report_panic(const char* file,
             const char* function, int line,
             const char* fmt, ...);

// log
#define info(fmt, ...) \
	report(source_file, \
	       source_function, \
	       source_line, "", fmt, ## __VA_ARGS__)

#define debug(fmt, ...) \
	report(source_file, \
	       source_function, \
	       source_line, "", fmt, ## __VA_ARGS__)

// panic
#define panic(fmt, ...) \
	report_panic(source_file, \
	             source_function, \
	             source_line, fmt, ## __VA_ARGS__)

#define oom() \
	panic("memory allocation failed")
