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

always_inline static inline int
compare_int32(int32_t a, int32_t b)
{
	return (a > b) - (a < b);
}

always_inline static inline int
compare_int64(int64_t a, int64_t b)
{
	return (a > b) - (a < b);
}

always_inline static inline int
compare_uint64(uint64_t a, uint64_t b)
{
	return (a > b) - (a < b);
}

always_inline static inline int
compare_float(float a, float b)
{
	return (a > b) - (a < b);
}

always_inline static inline int
compare_double(double a, double b)
{
	return (a > b) - (a < b);
}
