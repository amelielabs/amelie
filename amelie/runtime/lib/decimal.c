
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

#define DECIMAL_MAX_SCALE 18

static const int64_t decimal_pow10[19] =
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
	1000000000000000LL,
	10000000000000000LL,
	100000000000000000LL,
	1000000000000000000LL
};

static const double decimal_pow10_dbl[19] =
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
	1000000000000000.0,
	10000000000000000.0,
	100000000000000000.0,
	1000000000000000000.0
};

void
decimal_set(Decimal* result, Str* spec)
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

	if (unlikely(scale > DECIMAL_MAX_SCALE || !digits))
		goto error;

	result->value = value;
	result->scale = scale;
	return;
error:
	error("decimal overflow");
}

void
decimal_set_int(Decimal* result, int scale, int64_t value)
{
	if (unlikely(scale < 0 || scale > DECIMAL_MAX_SCALE))
		goto error;

	if (scale == 0)
	{
		result->value = value;
		result->scale = 0;
		return;
	}

	int64_t value_scaled;
	if (unlikely(int64_mul_overflow(&value_scaled, value, decimal_pow10[scale])))
		goto error;

	result->value = value_scaled;
	result->scale = scale;
	return;
error:
	error("decimal overflow");
}

void
decimal_set_double(Decimal* result, int scale, double value)
{
    if (unlikely(isnan(value) || isinf(value) || scale < 0 || scale > DECIMAL_MAX_SCALE))
		goto error;

	double value_scaled = value * decimal_pow10_dbl[scale];
	if (unlikely(value_scaled >=  9223372036854775808.0 ||
	             value_scaled <= -9223372036854775809.0))
		goto error;

    result->value = (int64_t)llround(value_scaled);
    result->scale = scale;
	return;
error:
	error("decimal overflow");
}

hot void
decimal_add(Decimal* result, Decimal* a, Decimal* b)
{
	if (likely(a->scale == b->scale))
	{
		int64_t value;
		if (unlikely(int64_add_overflow(&value, a->value, b->value)))
			goto error;
		result->value = value;
		result->scale = a->scale;
		return;
	}
	int scale;
	if (a->scale > b->scale)
		scale = a->scale;
	else
		scale = b->scale;
    int64_t a_scaled = a->value * decimal_pow10[scale - a->scale];
    int64_t b_scaled = b->value * decimal_pow10[scale - b->scale];
	int64_t value;
	if (unlikely(int64_add_overflow(&value, a_scaled, b_scaled)))
		goto error;
	result->value = value;
	result->scale = scale;
	return;
error:
	error("decimal add overflow");
}

hot void
decimal_addei(Decimal* result, Decimal* a, int64_t b)
{
	int64_t value;
	if (unlikely(int64_mul_overflow(&value, b, decimal_pow10[a->scale])))
		goto error;
	if (unlikely(int64_add_overflow(&value, a->value, value)))
		goto error;
	result->value = value;
	result->scale = a->scale;
	return;
error:
	error("decimal add overflow");
}

hot void
decimal_addef(double* result, Decimal* a, double b)
{
    double a_dbl = (double)a->value / decimal_pow10_dbl[a->scale];
	if (unlikely(double_add_overflow(result, a_dbl, b)))
		error("decimal add overflow");
}

hot void
decimal_sub(Decimal* result, Decimal* a, Decimal* b)
{
	if (likely(a->scale == b->scale))
	{
		int64_t value;
		if (unlikely(int64_sub_overflow(&value, a->value, b->value)))
			goto error;
		result->value = value;
		result->scale = a->scale;
		return;
	}
	int scale;
	if (a->scale > b->scale)
		scale = a->scale;
	else
		scale = b->scale;
    int64_t a_scaled = a->value * decimal_pow10[scale - a->scale];
    int64_t b_scaled = b->value * decimal_pow10[scale - b->scale];
	int64_t value;
	if (unlikely(int64_sub_overflow(&value, a_scaled, b_scaled)))
		goto error;
	result->value = value;
	result->scale = scale;
	return;
error:
	error("decimal sub overflow");
}

hot void
decimal_subei(Decimal* result, Decimal* a, int64_t b)
{
	int64_t value;
	if (unlikely(int64_mul_overflow(&value, b, decimal_pow10[a->scale])))
		goto error;
	if (unlikely(int64_sub_overflow(&value, a->value, value)))
		goto error;
	result->value = value;
	result->scale = a->scale;
	return;
error:
	error("decimal sub overflow");
}

hot void
decimal_subie(Decimal* result, int64_t a, Decimal* b)
{
	int64_t value;
	if (unlikely(int64_mul_overflow(&value, a, decimal_pow10[b->scale])))
		goto error;
	if (unlikely(int64_sub_overflow(&value, a, b->value)))
		goto error;
	result->value = value;
	result->scale = b->scale;
	return;
error:
	error("decimal sub overflow");
}

hot void
decimal_subef(double* result, Decimal* a, double b)
{
	double a_dbl = (double)a->value / decimal_pow10_dbl[a->scale];
	if (unlikely(double_sub_overflow(result, a_dbl, b)))
		error("decimal sub overflow");
}

hot void
decimal_subfe(double* result, double a, Decimal* b)
{
	double b_dbl = (double)b->value / decimal_pow10_dbl[b->scale];
	if (unlikely(double_sub_overflow(result, a, b_dbl)))
		error("decimal sub overflow");
}

hot void
decimal_mul(Decimal* result, Decimal* a, Decimal* b)
{
	__int128_t value = (__int128_t)a->value * (__int128_t)b->value;

	// rescale down if target scale exceeds max
	auto scale = a->scale + b->scale;
	for (; scale > DECIMAL_MAX_SCALE; scale--)
	{
		if (value >= 0)
			value = (value + 5) / 10;
		else
			value = (value - 5) / 10;
	}

	if (unlikely(value > INT64_MAX || value < INT64_MIN))
		error("decimal mul overflow");

	result->value = (int64_t)value;
	result->scale = scale;
}

hot void
decimal_mulei(Decimal* result, Decimal* a, int64_t b)
{
	int64_t value;
	if (unlikely(int64_mul_overflow(&value, a->value, b)))
		error("decimal mul overflow");
	result->value = value;
	result->scale = a->scale;
}

hot void
decimal_mulef(double* result, Decimal* a, double b)
{
    double a_dbl = (double)a->value / decimal_pow10_dbl[a->scale];
	if (unlikely(double_mul_overflow(result, a_dbl, b)))
		error("decimal mul overflow");
}

hot void
decimal_div(Decimal* result, Decimal* a, Decimal* b)
{
	if (unlikely(! b->value))
		error("decimal zero division");

	auto scale = a->scale;
	__int128_t mul = (__int128_t)decimal_pow10[b->scale] * 10;
	__int128_t div = (__int128_t)a->value * mul;
	__int128_t quotient = div / b->value;

	// rounding
	if (quotient >= 0)
		quotient = (quotient + 5) / 10;
	else
		quotient = (quotient - 5) / 10;

	if (unlikely(quotient > INT64_MAX || quotient < INT64_MIN))
		error("decimal div overflow");

	result->value = (int64_t)quotient;
	result->scale = scale;
}

hot void
decimal_divei(Decimal* result, Decimal* a, int64_t b)
{
	if (unlikely(! b))
		error("decimal zero division");

	__int128_t div = (__int128_t)a->value * 10;
	__int128_t quotient = div / b;

	// rouding
	if (quotient >= 0)
		quotient = (quotient + 5) / 10;
	else
		quotient = (quotient - 5) / 10;

	if (unlikely(quotient > INT64_MAX || quotient < INT64_MIN))
		error("decimal div overflow");

	result->value = (int64_t)quotient;
	result->scale = a->scale;
}

hot void
decimal_divie(Decimal* result, int64_t a, Decimal* b)
{
	if (unlikely(! b->value))
		error("decimal zero division");

	auto shift = b->scale * 2 + 1;
	__int128_t div = (__int128_t)a * decimal_pow10[shift];
	__int128_t quotient = div / b->value;

	// rouding
	if (quotient >= 0)
		quotient = (quotient + 5) / 10;
	else
		quotient = (quotient - 5) / 10;

	if (unlikely(quotient > INT64_MAX || quotient < INT64_MIN))
		error("decimal div overflow");

	result->value = (int64_t)quotient;
	result->scale = b->scale;
}

hot void
decimal_divef(double* result, Decimal* a, double b)
{
    double a_dbl = (double)a->value / decimal_pow10_dbl[a->scale];
	if (unlikely(double_div_overflow(result, a_dbl, b)))
		error("decimal div overflow");
}

hot void
decimal_divfe(double* result, double a, Decimal* b)
{
    double b_dbl = (double)b->value / decimal_pow10_dbl[b->scale];
	if (unlikely(double_div_overflow(result, a, b_dbl)))
		error("decimal div overflow");
}

void
decimal_modei(Decimal* result, Decimal* a, int64_t b)
{
	if (unlikely(! b))
		error("decimal zero division");

	int64_t value_scaled;
	if (unlikely(int64_mul_overflow(&value_scaled, b, decimal_pow10[a->scale])))
		error("decimal mod overflow");

	result->value = a->value % value_scaled;
	result->scale = a->scale;
}
