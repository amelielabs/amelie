
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

static inline void
interval_error(Str* str)
{
	error("invalid interval '%.*s'", str_size(str), str_of(str));
}

static inline void
interval_error_type(Str* str)
{
	error("invalid interval type '%.*s'", str_size(str), str_of(str));
}

static inline void
interval_error_field(Str* str)
{
	error("invalid interval field '%.*s'", str_size(str), str_of(str));
}

static inline void
interval_read_type(Interval* self, Str* type, int64_t value)
{
	switch (*str_of(type)) {
	case 'm':
		// minutes
		// minute
		// mins
		// min
		if (str_is(type, "mins", 4)    ||
		    str_is(type, "min", 3)     ||
		    str_is(type, "minutes", 7) ||
		    str_is(type, "minute", 6))
		{
			self->us += value * 60LL * 1000 * 1000;
			return;
		}

		// month
		// months
		if (str_is(type, "months", 6) ||
		    str_is(type, "month", 5))
		{
			self->m += value;
			return;
		}

		// milliseconds
		// millisecond
		// ms
		if (str_is(type, "ms", 2)            ||
		    str_is(type, "milliseconds", 12) ||
		    str_is(type, "millisecond", 11))
		{
			self->us += value * 1000LL;
			return;
		}

		// microseconds
		// microsecond
		if (str_is(type, "microseconds", 12) ||
		    str_is(type, "microsecond", 11))
		{
			self->us += value;
			return;
		}
		break;
	case 'u':
		// us
		if (str_is(type, "us", 2))
		{
			self->us += value;
			return;
		}
		break;
	case 's':
		// seconds
		// second
		// secs
		// sec
		if (str_is(type, "secs", 4)    ||
		    str_is(type, "sec", 3)     ||
		    str_is(type, "seconds", 7) ||
		    str_is(type, "second", 6))
		{
			self->us += value * 1000LL * 1000;
			return;
		}
		break;
	case 'h':
		// hours
		// hour
		// hrs	
		// hr
		if (str_is(type, "hrs", 3)   ||
		    str_is(type, "hr", 2)    ||
		    str_is(type, "hours", 5) ||
		    str_is(type, "hour", 4))
		{
			self->us += value * 60LL * 60 * 1000 * 1000;
			return;
		}
		break;
	case 'd':
		// days
		// day
		if (str_is(type, "days", 4) ||
		    str_is(type, "day", 3))
		{
			self->d += value;
			return;
		}
		break;
	case 'w':
		// weeks
		// week
		if (str_is(type, "weeks", 5) ||
		    str_is(type, "week", 4))
		{
			self->d += value * 7;
			return;
		}
		break;
	case 'y':
		// year
		// years
		if (str_is(type, "years", 5) ||
		    str_is(type, "year", 4))
		{
			self->m += value * 12;
			return;
		}
		break;
	default:
		break;
	}
	interval_error_type(type);
}

void
interval_set(Interval* self, Str* str)
{
	// <ws> [sign] <cardinal> <ws> <type> [ws ...]
	auto pos = str->pos;
	auto end = str->end;
	for (;;)
	{
		// whitespace
		while (pos < end && isspace(*pos))
			pos++;
		if (pos == end)
			break;

		// sign
		bool negative = false;
		if (*pos == '-')
		{
			negative = true;
			pos++;
		}

		// cardinal
		int64_t cardinal = 0;
		while (pos < end && isdigit(*pos))
		{
			if (unlikely(int64_mul_add_overflow(&cardinal, cardinal, 10, *pos - '0')))
				goto error;
			pos++;
		}
		if (pos == end || !isspace(*pos))
			goto error;
		pos++;
		if (negative)
			cardinal = -cardinal;

		// whitespace
		while (pos < end && isspace(*pos))
			pos++;
		if (pos == end)
			goto error;

		// type
		auto type_start = pos;
		while (pos < end && isalpha(*pos))
			pos++;
		Str type;
		str_set(&type, type_start, pos - type_start);

		// convert cardinal according to the type
		interval_read_type(self, &type, cardinal);
	}
	return;

error:
	interval_error(str);
}

int
interval_get(Interval* self, char* str, int str_size)
{
	// years/months
	const char* span;
	int size = 0;
	if (self->m != 0)
	{
		int m = self->m;
		int y = m / 12;
		if (y != 0)
		{
			span = abs(y) != 1? "years": "year";
			size += snprintf(str + size, str_size - size, "%d %s ", y, span);
			m = m % 12;
		}
		if (m != 0)
		{
			span = abs(m) != 1? "months": "month";
			size += snprintf(str + size, str_size - size, "%d %s ", m, span);
		}
	}

	// weeks/days
	if (self->d != 0)
	{
		int d = self->d;
		span = abs(d) != 1? "days": "day";
		size += snprintf(str + size, str_size - size, "%d %s ", d, span);
	}

	// hours
	int64_t us = self->us;
	int64_t hours = us / (60LL * 60 * 1000 * 1000);
	if (hours != 0)
	{
		span = llabs(hours) != 1? "hours": "hour";
		size += snprintf(str + size, str_size - size, "%" PRIi64 " %s ", hours, span);
		us = us % (60LL * 60 * 1000 * 1000);
	}

	// minutes
	int64_t minutes = us / (60LL * 1000 * 1000);
	if (minutes != 0)
	{
		span = llabs(minutes) != 1? "minutes": "minute";
		size += snprintf(str + size, str_size - size, "%" PRIi64 " %s ", minutes, span);
		us = us % (60LL * 1000 * 1000);
	}

	// seconds
	int64_t seconds = us / (1000LL * 1000);
	if (seconds != 0)
	{
		span = llabs(seconds) != 1? "seconds": "second";
		size += snprintf(str + size, str_size - size, "%" PRIi64 " %s ", seconds, span);
		us = us % (1000LL * 1000);
	}

	// milliseconds
	int64_t ms = us / 1000;
	if (ms != 0)
	{
		size += snprintf(str + size, str_size - size, "%" PRIi64 " ms ", ms);
		us = us % 1000;
	}

	// microseconds
	if (us != 0)
		size += snprintf(str + size, str_size - size, "%" PRIi64 " us ", us);

	// remove last space
	if (size > 0)
		size--;
	return size;
}

hot int
interval_compare(Interval* a, Interval* b)
{
	auto rc = compare_int64(a->m, b->m);
	if (rc != 0)
		return rc;
	rc = compare_int64(a->d, b->d);
	if (rc != 0)
		return rc;
	return compare_int64(a->us, b->us);
}

void
interval_add(Interval* result, Interval* a, Interval* b)
{
	result->m  = a->m + b->m;
	result->d  = a->d + b->d;
	result->us = a->us + b->us;
}

void
interval_sub(Interval* result, Interval* a, Interval* b)
{
	result->m  = a->m - b->m;
	result->d  = a->d - b->d;
	result->us = a->us - b->us;
}

void
interval_neg(Interval* result, Interval* a)
{
	result->m  = -a->m;
	result->d  = -a->d;
	result->us = -a->us;
}

typedef enum
{
	INTERVAL_YEAR,
	INTERVAL_MONTH,
	INTERVAL_DAY,
	INTERVAL_HOUR,
	INTERVAL_MINUTE,
	INTERVAL_SECOND,
	INTERVAL_MILLISECOND,
	INTERVAL_MICROSECOND
} IntervalField;

static inline int
interval_read_field(Str* type)
{
	switch (*str_of(type)) {
	case 'm':
		// minute
		if (str_is(type, "min", 3) ||
		    str_is(type, "minute", 6))
			return INTERVAL_MINUTE;

		// month
		if (str_is(type, "month", 5))
			return INTERVAL_MONTH;

		// millisecond
		// milliseconds
		// ms
		if (str_is(type, "ms", 2)            ||
		    str_is(type, "milliseconds", 12) ||
		    str_is(type, "millisecond", 11))
			return INTERVAL_MILLISECOND;

		// microseconds
		// microsecond
		if (str_is(type, "microseconds", 12) ||
		    str_is(type, "microsecond", 11))
			return INTERVAL_MICROSECOND;
		break;
	case 'u':
		// us
		if (str_is(type, "us", 2))
			return INTERVAL_MICROSECOND;
		break;
	case 's':
		// second
		// sec
		if (str_is(type, "sec", 3) ||
		    str_is(type, "second", 6))
			return INTERVAL_SECOND;
		break;
	case 'h':
		// hour
		// hr
		if (str_is(type, "hr", 2)    ||
		    str_is(type, "hour", 4))
			return INTERVAL_HOUR;
		break;
	case 'd':
		// day
		if (str_is(type, "day", 3))
			return INTERVAL_DAY;
		break;
	case 'y':
		// year
		if (str_is(type, "year", 4))
			return INTERVAL_YEAR;
		break;
	default:
		break;
	}
	return -1;
}

void
interval_trunc(Interval* self, Str* field)
{
	int rc = interval_read_field(field);
	if (rc == -1)
		interval_error_field(field);

	switch (rc) {
	case INTERVAL_YEAR:
	{
		self->m  = (self->m / 12) * 12;
		self->d  = 0;
		self->us = 0;
		break;
	}
	case INTERVAL_MONTH:
		self->d  = 0;
		self->us = 0;
		break;
	case INTERVAL_DAY:
	{
		const auto hour = 24ULL * 60 * 60 * 1000 * 1000;
		self->us = (self->us / hour) * hour;
		break;
	}
	case INTERVAL_HOUR:
	{
		const auto min = 60ULL * 60 * 1000 * 1000;
		self->us = (self->us / min) * min;
		break;
	}
	case INTERVAL_MINUTE:
	{
		const auto sec = 60ULL * 1000 * 1000;
		self->us = (self->us / sec) * sec;
		break;
	}
	case INTERVAL_SECOND:
	{
		const auto sec = 1000ULL * 1000;
		self->us = (self->us / sec) * sec;
		break;
	}
	case INTERVAL_MILLISECOND:
	{
		const auto ms = 1000ULL;
		self->us = (self->us / ms) * ms;
		break;
	}
	case INTERVAL_MICROSECOND:
		break;
	}
}

uint64_t
interval_extract(Interval* self, Str* field)
{
	int rc = interval_read_field(field);
	if (rc == -1)
		interval_error_field(field);

	uint64_t result;
	switch (rc) {
	case INTERVAL_YEAR:
		result = self->m / 12;
		break;
	case INTERVAL_MONTH:
		result = self->m % 12;
		break;
	case INTERVAL_DAY:
		result = self->d;
		break;
	case INTERVAL_HOUR:
		result = (self->us / (60ULL * 60 * 1000 * 1000));
		break;
	case INTERVAL_MINUTE:
		result = (self->us % (60ULL * 60 * 1000 * 1000)) / (60ULL * 1000 * 1000);
		break;
	case INTERVAL_SECOND:
		result = (self->us % (60ULL * 1000 * 1000)) / (1000ULL * 1000);
		break;
	case INTERVAL_MILLISECOND:
		result = (self->us % (1000ULL * 1000)) / (1000ULL);
		break;
	case INTERVAL_MICROSECOND:
		result = (self->us % (1000));
		break;
	}
	return result;
}
