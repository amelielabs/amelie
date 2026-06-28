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

always_inline static inline bool
double_add_overflow(double* result, double a, double b)
{
	*result = a + b;
	return unlikely(__builtin_isinf(*result) &&
	               !__builtin_isinf(a) &&
	               !__builtin_isinf(b));
}

always_inline static inline bool
double_sub_overflow(double* result, double a, double b)
{
	*result = a - b;
	return unlikely(__builtin_isinf(*result) &&
	               !__builtin_isinf(a) &&
	               !__builtin_isinf(b));
}

always_inline static inline bool
double_mul_overflow(double* result, double a, double b)
{
	*result = a * b;
	if (unlikely(__builtin_isinf(*result) &&
	            !__builtin_isinf(a) &&
	            !__builtin_isinf(b)))
		return true;
	return unlikely(*result == 0.0 && a != 0.0 && b != 0.0);
}

always_inline static inline bool
double_div_overflow(double* result, double a, double b)
{
	*result = a / b;
	if (unlikely(__builtin_isinf(*result) &&
	            !__builtin_isinf(a)))
		return true;
	return unlikely(*result == 0.0 && a != 0.0 && !__builtin_isinf(b));
}
