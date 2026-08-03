
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

#include <amelie_base.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include "base/overflow_fp.h"

static const int64_t decimal_pow10[16] =
{
	1LL,
	10LL,
	100LL,
	1000LL,
	10000LL,
	100000LL,
	1000000LL,
	10000000LL,
	100000000LL,
	1000000000LL,
	10000000000LL,
	100000000000LL,
	1000000000000LL,
	10000000000000LL,
	100000000000000LL,
	1000000000000000LL
};

static const double decimal_pow10_dbl[16] =
{
	1.0,
	10.0,
	100.0,
	1000.0,
	10000.0,
	100000.0,
	1000000.0,
	10000000.0,
	100000000.0,
	1000000000.0,
	10000000000.0,
	100000000000.0,
	1000000000000.0,
	10000000000000.0,
	100000000000000.0,
	1000000000000000.0
};

hot uint64_t
decimal_add(uint64_t a, uint64_t b)
{
	// decimal + decimal
	auto a_value = decimal_value(a);
	auto a_scale = decimal_scale(a);
	auto b_value = decimal_value(b);
	auto b_scale = decimal_scale(b);

	if (likely(a_scale == b_scale))
	{
		int64_t value;
		if (unlikely(int64_add_overflow(&value, a_value, b_value)))
			goto error;
		if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
			goto error;
		return decimal_set(value, a_scale);
	}

	// scale values to match the max scale
	if (a_scale < b_scale)
	{
		if (unlikely(int64_mul_overflow(&a_value, a_value, decimal_pow10[b_scale - a_scale])))
			goto error;
		a_scale = b_scale;
	} else {
		if (unlikely(int64_mul_overflow(&b_value, b_value, decimal_pow10[a_scale - b_scale])))
			goto error;
	}

	int64_t value;
	if (unlikely(int64_add_overflow(&value, a_value, b_value)))
		goto error;

	if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
		goto error;

	return decimal_set(value, a_scale);

error:
	error("decimal add overflow");
}

hot uint64_t
decimal_addei(uint64_t a, int64_t b)
{
	// decimal + int
	auto    a_value = decimal_value(a);
	auto    a_scale = decimal_scale(a);
	int64_t b_upscale;
	if (unlikely(int64_mul_overflow(&b_upscale, b, decimal_pow10[a_scale])))
		goto error;

	int64_t value;
	if (unlikely(int64_add_overflow(&value, a_value, b_upscale)))
		goto error;

	if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
		goto error;

	return decimal_set(value, a_scale);

error:
	error("decimal add overflow");
}

hot uint64_t
decimal_sub(uint64_t a, uint64_t b)
{
	// decimal - decimal
	auto a_value = decimal_value(a);
	auto a_scale = decimal_scale(a);
	auto b_value = decimal_value(b);
	auto b_scale = decimal_scale(b);

	if (likely(a_scale == b_scale))
	{
		int64_t value;
		if (unlikely(int64_sub_overflow(&value, a_value, b_value)))
			goto error;
		if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
			goto error;
		return decimal_set(value, a_scale);
	}

	// scale values to match the max scale
	if (a_scale < b_scale)
	{
		if (unlikely(int64_mul_overflow(&a_value, a_value, decimal_pow10[b_scale - a_scale])))
			goto error;
		a_scale = b_scale;
	} else {
		if (unlikely(int64_mul_overflow(&b_value, b_value, decimal_pow10[a_scale - b_scale])))
			goto error;
	}

	int64_t value;
	if (unlikely(int64_sub_overflow(&value, a_value, b_value)))
		goto error;

	if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
		goto error;

	return decimal_set(value, a_scale);

error:
	error("decimal sub overflow");
}

hot uint64_t
decimal_subei(uint64_t a, int64_t b)
{
	// decimal - int
	auto    a_value = decimal_value(a);
	auto    a_scale = decimal_scale(a);
	int64_t b_upscale;
	if (unlikely(int64_mul_overflow(&b_upscale, b, decimal_pow10[a_scale])))
		goto error;

	int64_t value;
	if (unlikely(int64_sub_overflow(&value, a_value, b_upscale)))
		goto error;

	if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
		goto error;

	return decimal_set(value, a_scale);

error:
	error("decimal sub overflow");
}

hot uint64_t
decimal_subie(int64_t a, uint64_t b)
{
	// int - decimal
	auto    b_value = decimal_value(b);
	auto    b_scale = decimal_scale(b);
	int64_t a_upscale;
	if (unlikely(int64_mul_overflow(&a_upscale, a, decimal_pow10[b_scale])))
		goto error;

	int64_t value;
	if (unlikely(int64_sub_overflow(&value, a_upscale, b_value)))
		goto error;

	if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
		goto error;

	return decimal_set(value, b_scale);

error:
	error("decimal sub overflow");
}

hot uint64_t
decimal_mul(uint64_t a, uint64_t b)
{
	// decimal * decimal
	auto a_value = decimal_value(a);
	auto a_scale = decimal_scale(a);
	auto b_value = decimal_value(b);
	auto b_scale = decimal_scale(b);

	__int128_t mul = (__int128_t)a_value * (__int128_t)b_value;

	// scale down
	uint32_t scale = a_scale + b_scale;
	for (; scale > DECIMAL_MAX_SCALE; scale--)
	{
		if (mul >= 0)
			mul = (mul + 5) / 10;
		else
			mul = (mul - 5) / 10;
	}

	if (unlikely(mul > DECIMAL_MAX || mul < DECIMAL_MIN))
		error("decimal mul overflow");

	return decimal_set((int64_t)mul, scale);
}

hot uint64_t
decimal_mulei(uint64_t a, int64_t b)
{
	// decimal * int
	auto a_value = decimal_value(a);
	auto a_scale = decimal_scale(a);

	__int128_t mul = (__int128_t)a_value * (__int128_t)b;
	if (unlikely(mul > DECIMAL_MAX || mul < DECIMAL_MIN))
		error("decimal mul overflow");

	return decimal_set((int64_t)mul, a_scale);
}

hot uint64_t
decimal_div(uint64_t a, uint64_t b)
{
	// decimal / decimal
	auto a_value = decimal_value(a);
	auto a_scale = decimal_scale(a);
	auto b_value = decimal_value(b);
	auto b_scale = decimal_scale(b);

	if (unlikely(! b_value))
		error("decimal zero division");

	// scale up
	__int128_t mul = (__int128_t)decimal_pow10[b_scale] * 10;
	__int128_t div = (__int128_t)a_value * mul;
	__int128_t quotient = div / b_value;

	// rounding
	if (quotient >= 0)
		quotient = (quotient + 5) / 10;
	else
		quotient = (quotient - 5) / 10;

	if (unlikely(quotient > DECIMAL_MAX || quotient < DECIMAL_MIN))
		error("decimal div overflow");

	return decimal_set((int64_t)quotient, a_scale);
}

hot uint64_t
decimal_divei(uint64_t a, int64_t b)
{
	// decimal / int
	auto a_value = decimal_value(a);
	auto a_scale = decimal_scale(a);

	if (unlikely(! b))
		error("decimal zero division");

	__int128_t div = (__int128_t)a_value * 10;
	__int128_t quotient = div / b;

	// rounding
	if (quotient >= 0)
		quotient = (quotient + 5) / 10;
	else
		quotient = (quotient - 5) / 10;

	if (unlikely(quotient > DECIMAL_MAX || quotient < DECIMAL_MIN))
		error("decimal div overflow");

	return decimal_set((int64_t)quotient, a_scale);
}

hot uint64_t
decimal_divie(int64_t a, uint64_t b)
{
	// int / decimal
	auto b_value = decimal_value(b);
	auto b_scale = decimal_scale(b);

	if (unlikely(! b_value))
		error("decimal zero division");

	// upscale
	__int128_t pow_b = decimal_pow10[b_scale];
	__int128_t div   = (__int128_t)a * pow_b * pow_b * 10;
	__int128_t quotient = div / b_value;

	// rounding
	if (quotient >= 0)
		quotient = (quotient + 5) / 10;
	else
		quotient = (quotient - 5) / 10;

	if (unlikely(quotient > DECIMAL_MAX || quotient < DECIMAL_MIN))
		error("decimal div overflow");

	return decimal_set((int64_t)quotient, b_scale);
}

hot uint64_t
decimal_modei(uint64_t a, int64_t b)
{
	// decimal % int
	int64_t  a_value = decimal_value(a);
	uint32_t a_scale = decimal_scale(a);

	if (unlikely(! b))
		error("decimal zero div");

	int64_t b_upscale;
	if (unlikely(int64_mul_overflow(&b_upscale, b, decimal_pow10[a_scale])))
		error("decimal mod overflow");

	return decimal_set(a_value % b_upscale, a_scale);
}

uint64_t
decimal_set_str(Str* spec)
{
	if (unlikely(str_empty(spec)))
		goto error;

	auto    sign   = false;
	int64_t value  = 0;
	auto    scale  = 0;
	auto    dot    = false;
	auto    digits = false;

	// -+
	auto pos = spec->pos;
	auto end = spec->end;
	if (*pos == '-') {
		sign = true;
		pos++;
	} else
	if (*pos == '+') {
		pos++;
	}

	// [-+]0-9[.0-9]
	for (; pos < end; pos++)
	{
		auto at = *pos;
		if (at >= '0' && at <= '9')
		{
			digits = true;

			// ignore zeroes after scale max
			auto digit = at - '0';
			if (unlikely(dot && scale >= DECIMAL_MAX_SCALE))
			{
				if (digit == 0)
					continue;
				goto error;
			}

			if (! sign)
			{
				if (unlikely(int64_mul_add_overflow(&value, value, 10, digit)))
					goto error;
			} else
			{
				if (unlikely(int64_mul_sub_overflow(&value, value, 10, digit)))
					goto error;
			}

			// .scale
            if (dot)
				scale++;
			continue;
        }

		if (at == '.' && !dot)
		{
			dot = true;
			continue;
		}

		goto error;
	}

	if (unlikely(value > DECIMAL_MAX || value < DECIMAL_MIN))
		goto error;

	if (unlikely(scale > DECIMAL_MAX_SCALE || !digits))
		goto error;

	return decimal_set(value, scale);

error:
	error("decimal overflow");
}

uint64_t
decimal_set_int(int scale, int64_t value)
{
	if (unlikely(scale < 0 || scale > DECIMAL_MAX_SCALE))
		goto error;

	if (scale == 0)
		return decimal_set(value, 0);

	int64_t value_scaled;
	if (unlikely(int64_mul_overflow(&value_scaled, value, decimal_pow10[scale])))
		goto error;
	if (unlikely(value_scaled > DECIMAL_MAX ||
	             value_scaled < DECIMAL_MIN))
		goto error;

	return decimal_set(value_scaled, scale);

error:
	error("decimal overflow");
}

uint64_t
decimal_set_double(int scale, double value)
{
	if (unlikely(isnan(value) || isinf(value) || scale < 0 || scale > DECIMAL_MAX_SCALE))
		goto error;

	double value_scaled = value * decimal_pow10_dbl[scale];
	if (unlikely(value_scaled > DECIMAL_MAX ||
	             value_scaled < DECIMAL_MIN))
		goto error;

	return decimal_set((int64_t)llround(value_scaled), scale);

error:
	error("decimal overflow");
}

hot int
decimal_compare(uint64_t a, uint64_t b)
{
	if (a == b)
		return 0;

	auto a_scale = decimal_scale(a);
	auto b_scale = decimal_scale(b);
	auto a_value = decimal_value(a);
	auto b_value = decimal_value(b);
	if (likely(a_scale == b_scale))
		return compare_int64(a_value, b_value);

	// scale values to match the max scale
	__int128 a_128;
	__int128 b_128;
	if (a_scale < b_scale)
	{
		a_128 = (__int128)a_value * decimal_pow10[b_scale - a_scale];
		b_128 = (__int128)b_value;
	} else
	{
		a_128 = (__int128)a_value;
		b_128 = (__int128)b_value * decimal_pow10[a_scale - b_scale];
	}
	return (a_128 > b_128) - (a_128 < b_128);
}
