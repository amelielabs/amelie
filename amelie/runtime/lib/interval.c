
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

static inline void
interval_read_type(Interval* self, Str* type, int64_t value)
{
	switch (*str_of(type)) {
	case 'm':
		// minutes
		// minute
		// min
		if (str_compare_raw(type, "min", 3) ||
		    str_compare_raw(type, "minutes", 7) ||
		    str_compare_raw(type, "minute", 6))
		{
			self->us += value * 60LL * 1000 * 1000;
			return;
		}

		// month
		// months
		if (str_compare_raw(type, "months", 6) ||
		    str_compare_raw(type, "month", 5))
		{
			self->m += value;
			return;
		}

		// milliseconds
		// millisecond
		// ms
		if (str_compare_raw(type, "ms", 2)            ||
		    str_compare_raw(type, "milliseconds", 12) ||
		    str_compare_raw(type, "millisecond", 11))
		{
			self->us += value * 1000LL;
			return;
		}

		// microseconds
		// microsecond
		if (str_compare_raw(type, "microseconds", 12) ||
		    str_compare_raw(type, "microsecond", 11))
		{
			self->us += value;
			return;
		}
		break;
	case 'u':
		// us
		if (str_compare_raw(type, "us", 2))
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
		if (str_compare_raw(type, "secs", 4)    ||
		    str_compare_raw(type, "sec", 3)     ||
		    str_compare_raw(type, "seconds", 7) ||
		    str_compare_raw(type, "second", 6))
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
		if (str_compare_raw(type, "hrs", 3)   ||
		    str_compare_raw(type, "hr", 2)    ||
		    str_compare_raw(type, "hours", 5) ||
		    str_compare_raw(type, "hour", 4))
		{
			self->us += value * 60LL * 60 * 1000 * 1000;
			return;
		}
		break;
	case 'd':
		// days
		// day
		if (str_compare_raw(type, "days", 4) ||
		    str_compare_raw(type, "day", 3))
		{
			self->d += value;
			return;
		}
		break;
	case 'w':
		// weeks
		// week
		if (str_compare_raw(type, "weeks", 5) ||
		    str_compare_raw(type, "week", 4))
		{
			self->d += value * 7;
			return;
		}
		break;
	case 'y':
		// year
		// years
		if (str_compare_raw(type, "years", 5) ||
		    str_compare_raw(type, "year", 4))
		{
			self->m += value * 12;
			return;
		}
		break;
	default:
		break;
	}
	error("malformed interval string");
}

void
interval_read(Interval* self, Str* str)
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
			cardinal = (cardinal * 10) + (*pos - '0');
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
	error("malformed interval string");
}

int
interval_write(Interval* self, char* str, int str_size)
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
			span = y != 1? "years": "year";
			size += snprintf(str + size, str_size - size, "%d %s ", y, span);
			m = m % 12;
		}
		if (m != 0)
		{
			span = m != 1? "months": "month";
			size += snprintf(str + size, str_size - size, "%d %s ", m, span);
		}
	}

	// weeks/days
	if (self->d != 0)
	{
		int d = self->d;
		span = d != 1? "days": "day";
		size += snprintf(str + size, str_size - size, "%d %s ", d, span);
	}

	// hours
	int64_t us = self->us;
	int64_t hours = us / (60LL * 60 * 1000 * 1000);
	if (hours != 0)
	{
		span = hours != 1? "hours": "hour";
		size += snprintf(str + size, str_size - size, "%" PRIi64 " %s ", hours, span);
		us = us % (60LL * 60 * 1000 * 1000);
	}

	// minutes
	int64_t minutes = us / (60LL * 1000 * 1000);
	if (minutes != 0)
	{
		span = minutes != 1? "minutes": "minute";
		size += snprintf(str + size, str_size - size, "%" PRIi64 " %s ", minutes, span);
		us = us % (60LL * 1000 * 1000);
	}

	// seconds
	int64_t seconds = us / (1000LL * 1000);
	if (seconds != 0)
	{
		span = seconds != 1? "seconds": "second";
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

typedef enum
{
	INTERVAL_YEAR,
	INTERVAL_QUARTER,
	INTERVAL_MONTH,
	INTERVAL_WEEK,
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
		if (str_compare_raw(type, "minute", 6))
			return INTERVAL_MINUTE;

		// month
		if (str_compare_raw(type, "month", 5))
			return INTERVAL_MONTH;

		// millisecond
		// milliseconds
		// ms
		if (str_compare_raw(type, "ms", 2) ||
		    str_compare_raw(type, "milliseconds", 12) ||
		    str_compare_raw(type, "millisecond", 11))
			return INTERVAL_MILLISECOND;

		// microseconds
		// microsecond
		if (str_compare_raw(type, "microseconds", 12) ||
		    str_compare_raw(type, "microsecond", 11))
			return INTERVAL_MICROSECOND;
		break;
	case 'u':
		// us
		if (str_compare_raw(type, "us", 2))
			return INTERVAL_MICROSECOND;
		break;
	case 's':
		// second
		// sec
		if (str_compare_raw(type, "sec", 3) ||
		    str_compare_raw(type, "second", 6))
			return INTERVAL_SECOND;
		break;
	case 'h':
		// hour
		// hr
		if (str_compare_raw(type, "hr", 2)    ||
		    str_compare_raw(type, "hour", 4))
			return INTERVAL_HOUR;
		break;
	case 'd':
		// day
		if (str_compare_raw(type, "day", 3))
			return INTERVAL_DAY;
		break;
	case 'w':
		// week
		if (str_compare_raw(type, "week", 4))
			return INTERVAL_WEEK;
		break;
	case 'q':
		// quarter
		if (str_compare_raw(type, "quarter", 7))
			return INTERVAL_QUARTER;
	case 'y':
		// year
		if (str_compare_raw(type, "year", 4))
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
		error("unknown interval truncate field");

	if (rc == INTERVAL_WEEK)
		error("cannot truncate interval by week");

	switch (rc) {
	case INTERVAL_YEAR:
	{
		self->m  = (self->m / 12) * 12;
		self->d  = 0;
		self->us = 0;
		break;
	}
	case INTERVAL_QUARTER:
		self->m  = (self->m / 3) * 3;
		self->d  = 0;
		self->us = 0;
		break;
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
