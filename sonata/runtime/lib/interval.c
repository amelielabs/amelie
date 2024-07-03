
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>

static inline void
interval_read_type(Interval* self, Str* type, uint64_t value)
{
	switch (*str_of(type)) {
	case 'm':
		// minutes
		// minute
		if (str_compare_raw(type, "minutes", 7) ||
			str_compare_raw(type, "minute", 6))
		{
			self->us += value * 60ULL * 1000 * 1000;
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
		if (str_compare_raw(type, "ms", 2) ||
			str_compare_raw(type, "milliseconds", 12) ||
			str_compare_raw(type, "millisecond", 11))
		{
			self->us += value * 1000ULL;
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
		if (str_compare_raw(type, "secs", 4) ||
			str_compare_raw(type, "sec", 3) ||
			str_compare_raw(type, "seconds", 7) ||
			str_compare_raw(type, "second", 6))
		{
			self->us += value * 1000ULL * 1000;
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
			self->us += value * 60ULL * 60 * 1000 * 1000;
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
	auto pos = str->pos;
	auto end = str->end;
	for (;;)
	{
		// whitespace
		while (pos < end && isspace(*pos))
			pos++;
		if (pos == end)
			break;

		// <ws> <cardinal> <ws> <type> [ws ...]
		uint64_t cardinal = 0;
		while (pos < end && isdigit(*pos))
		{
			cardinal = (cardinal * 10) + (*pos - '0');
			pos++;
		}
		if (pos == end || !isspace(*pos))
			goto error;
		pos++;

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
	int size = 0;
	if (self->m > 0)
	{
		int m = self->m;
		int y = m / 12;
		if (y > 0)
		{
			size += snprintf(str + size, str_size - size, "%d years ", y);
			m = m % 12;
		}
		if (m > 0)
			size += snprintf(str + size, str_size - size, "%d months ", m);
	}

	// weeks/days
	if (self->d > 0)
	{
		int d = self->d;
		int w = d / 7;
		if (w > 0)
		{
			size += snprintf(str + size, str_size - size, "%d weeks ", w);
			d = d % 7;
		}
		if (d > 0)
			size += snprintf(str + size, str_size - size, "%d days ", d);
	}

	// hours
	uint64_t us = self->us;
	uint64_t hours = us / (60ULL * 60 * 1000 * 1000);
	if (hours > 0)
	{
		size += snprintf(str + size, str_size - size, "%" PRIu64 " hours ", hours);
		us = us % (60ULL * 60 * 1000 * 1000);
	}

	// minutes
	uint64_t minutes = us / (60ULL * 1000 * 1000);
	if (minutes > 0)
	{
		size += snprintf(str + size, str_size - size, "%" PRIu64 " minutes ", minutes);
		us = us % (60ULL * 1000 * 1000);
	}

	// seconds
	uint64_t seconds = us / (1000ULL * 1000);
	if (seconds > 0)
	{
		size += snprintf(str + size, str_size - size, "%" PRIu64 " seconds ", seconds);
		us = us % (1000ULL * 1000);
	}

	// milliseconds
	uint64_t ms = us / 1000;
	if (ms > 0)
	{
		size += snprintf(str + size, str_size - size, "%" PRIu64 " ms ", ms);
		us = us % 1000;
	}

	// microseconds
	if (us > 0)
		size += snprintf(str + size, str_size - size, "%" PRIu64 " us ", us);

	// remove last space
	if (size > 0)
	{
		str[size] = 0;
		size--;
	}
	return size;
}
