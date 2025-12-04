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

static inline int
vsfmt(char*restrict str, int str_size, const char* fmt, va_list args)
{
	if (unlikely(! str_size))
		return 0;
	auto size = vsnprintf(str, str_size, fmt, args);
	if (unlikely(size >= str_size))
		size = str_size - 1;
	str[str_size - 1] =  0;
	return size;
}

static inline int format_validate(3, 4)
sfmt(char*restrict str, int str_size, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	auto size = vsfmt(str, str_size, fmt, args);
	va_end(args);
	return size;
}
