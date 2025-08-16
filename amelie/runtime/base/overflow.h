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
int32_add_overflow(int32_t* result, int32_t a, int32_t b)
{
	auto result_ptr = result;
	return __builtin_sadd_overflow(a, b, result_ptr);
}

always_inline static inline bool
int32_sub_overflow(int32_t* result, int32_t a, int32_t b)
{
	auto result_ptr = result;
	return __builtin_ssub_overflow(a, b, result_ptr);
}

always_inline static inline bool
int32_mul_overflow(int32_t* result, int32_t a, int32_t b)
{
	auto result_ptr = result;
	return __builtin_smul_overflow(a, b, result_ptr);
}

always_inline static inline bool
int64_add_overflow(int64_t* result, int64_t a, int64_t b)
{
	auto result_ptr = (long long*)result;
	return __builtin_saddll_overflow(a, b, result_ptr);
}

always_inline static inline bool
int64_sub_overflow(int64_t* result, int64_t a, int64_t b)
{
	auto result_ptr = (long long*)result;
	return __builtin_ssubll_overflow(a, b, result_ptr);
}

always_inline static inline bool
int64_mul_overflow(int64_t* result, int64_t a, int64_t b)
{
	auto result_ptr = (long long*)result;
	return __builtin_smulll_overflow(a, b, result_ptr);
}

always_inline static inline bool
int32_mul_add_overflow(int32_t* result, int32_t a, int32_t mul, int32_t add)
{
	if (unlikely(int32_mul_overflow(result, a, mul)))
		return true;
	return int32_add_overflow(result, *result, add);
}

always_inline static inline bool
int64_mul_add_overflow(int64_t* result, int64_t a, int64_t mul, int64_t add)
{
	if (unlikely(int64_mul_overflow(result, a, mul)))
		return true;
	return int64_add_overflow(result, *result, add);
}

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
