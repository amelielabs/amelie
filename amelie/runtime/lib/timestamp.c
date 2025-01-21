
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
timestamp_error_str(Str* str)
{
	error("invalid timestamp '%.*s'", str_size(str), str_of(str));
}

static inline void
timestamp_error(void)
{
	error("invalid timestamp");
}

hot static inline int
timestamp_parse_part(Str* str, char* pos, char* pos_end)
{
	int32_t value = 0;
	while (pos < pos_end && isdigit(*pos))
	{
		if (unlikely(int32_mul_add_overflow(&value, value, 10, *pos - '0')))
			timestamp_error_str(str);
		pos++;
	}
	if (pos != pos_end)
		timestamp_error_str(str);
	return value;
}

hot static void
timestamp_parse(Timestamp* self, Str* str)
{
	// yyyy-mm-dd< |T>hh:mm:ss[.ssssss][+/-hh[:mm]|Z|]
	auto pos = str->pos;
	auto end = str->end;

	// yyyy-mm-dd hh:mm:ss (minimum allowed)
	if (unlikely(str_size(str) < 19))
		goto error;

	// whitespace
	while (pos < end && isspace(*pos))
		pos++;
	if (pos == end)
		goto error;

	// year
	auto pos_end = pos + 4;
	if (unlikely(*pos_end != '-'))
		goto error;
	self->time.tm_year = timestamp_parse_part(str, pos, pos_end);
	pos += 5;

	// month
	pos_end = pos + 2;
	if (unlikely(*pos_end != '-'))
		goto error;
	self->time.tm_mon = timestamp_parse_part(str, pos, pos_end);
	pos += 3;

	// day
	pos_end = pos + 2;
	if (unlikely(*pos_end != ' ' && *pos_end != 'T'))
		goto error;
	self->time.tm_mday = timestamp_parse_part(str, pos, pos_end);
	pos += 3;

	// hour
	pos_end = pos + 2;
	if (unlikely(*pos_end != ':'))
		goto error;
	self->time.tm_hour = timestamp_parse_part(str, pos, pos_end);
	pos += 3;

	// mm
	pos_end = pos + 2;
	if (unlikely(*pos_end != ':'))
		goto error;
	self->time.tm_min = timestamp_parse_part(str, pos, pos_end);
	pos += 3;

	// ss
	pos_end = pos + 2;
	self->time.tm_sec = timestamp_parse_part(str, pos, pos_end);
	pos += 2;

	// [.ssssss]
	if (pos != end && *pos == '.')
	{
		pos++;
		int resolution = 0;
		while (pos < end && isdigit(*pos))
		{
			if (unlikely(int32_mul_add_overflow(&self->us, self->us, 10, *pos - '0')))
				goto error;
			resolution++;
			pos++;
		}
		// convert to us
		switch (resolution) {
		case 1: self->us *= 100000;
			break;
		case 2: self->us *= 10000;
			break;
		case 3: self->us *= 1000;
			break;
		case 4: self->us *= 100;
			break;
		case 5: self->us *= 10;
			break;
		case 6:
			break;
		case 0:
		default:
			goto error;
		}
	}

	// eof (local time)
	if (pos == end)
		return;

	// Zulu (UTC)
	if (*pos == 'Z')
	{
		self->zone_set = true;
		self->zone = 0;
		pos++;
		if (unlikely(pos != end))
			goto error;
		return;
	}

	// +/-hh
	pos_end = pos + 3;
	if (unlikely(pos_end > end))
		goto error;

	// +/-
	bool negative = false;	
	if (*pos == '-')
		negative = true;
	else
	if (unlikely(*pos != '+'))
		goto error;
	pos++;

	// hh
	int hh = timestamp_parse_part(str, pos, pos_end);
	pos += 2;

	// [:]mm
	int mm = 0;
	if (pos != end)
	{
		if (*pos == ':')
			pos++;
		pos_end = pos + 2;
		if (unlikely(pos_end != end))
			goto error;
		mm = timestamp_parse_part(str, pos, pos_end);
	}

	// store timezone offset in seconds
	self->zone = hh * 60 * 60 + mm * 60;
	if (negative)
		self->zone = -self->zone;

	self->zone_set = true;
	return;

error:
	timestamp_error_str(str);
}

hot static inline void
timestamp_validate(Timestamp* self, Str* str)
{
	// validate fields

	// year (starting from unix time)
	if (! (self->time.tm_year >= 1970 && self->time.tm_year <= 9999))
		goto error;

	// month
	if (! (self->time.tm_mon >= 1 && self->time.tm_mon <= 12))
		goto error;

	// day
	if (! (self->time.tm_mday >= 1 && self->time.tm_mday <= 31))
		goto error;

	// zone
	if (self->zone_set)
	{
		if (self->zone > 0)
		{
			if (! (self->zone >= 0 && self->zone <= 24 * 60 * 60))
				goto error;
		} else
		{
			if (self->zone < -24 * 60 * 60)
				goto error;
		}
	}
	return;

error:
	timestamp_error_str(str);
}

hot static inline void
timestamp_validate_overflow(int64_t value)
{
	if (unlikely(value < TIMESTAMP_MIN || value > TIMESTAMP_MAX))
		error("timestamp overflow");
}

hot void
timestamp_set_date(Timestamp* self, int64_t julian)
{
	int year, month, day;
	date_get_gregorian(julian, &year, &month, &day);

	// set time with corrections for mktime, this might
	// overflow on get if timestamp becomes
	// out of range
	self->time.tm_year = year  - 1900;
	self->time.tm_mon  = month - 1;
	self->time.tm_mday = day;
}

hot void
timestamp_set(Timestamp* self, Str* str)
{
	// parse iso8601 with microsends
	timestamp_parse(self, str);

	// validate result
	timestamp_validate(self, str);

	// do corrections to support mktime
	self->time.tm_year -= 1900;
	self->time.tm_mon--;
}

hot void
timestamp_set_unixtime(Timestamp* self, int64_t value)
{
	timestamp_validate_overflow(value);
	// UTC time in seconds
	time_t time = value / 1000000ULL;
	self->us = value % 1000000ULL;
	if (! gmtime_r(&time, &self->time))
		timestamp_error();
}

hot int64_t
timestamp_get_unixtime(Timestamp* self, Timezone* timezone)
{
	// mktime
	self->time.tm_isdst = -1;	
	int64_t time = mktime(&self->time);
	if (unlikely(time == (time_t)-1))
		timestamp_error();

	auto zone_set = self->zone_set;
	if (timezone)
	{
		if (! zone_set)
		{
			auto tztime = timezone_match(timezone, time);
			assert(tztime);
			time -= tztime->utoff;
		}
	} else
	{
		// do not apply timezone adjustment if timezone is
		// not supplied
		zone_set = false;
	}

	if (zone_set)
		time -= self->zone;

	if (unlikely(time < 0))
		error("timestamp overflow");

	int64_t value = time * 1000000ULL; // convert to us
	value += self->us;
	timestamp_validate_overflow(value);
	return value;
}

hot int
timestamp_get(int64_t value, Timezone* timezone, char* str, int str_size)
{
	// UTC time in seconds
	timestamp_validate_overflow(value);
	time_t time = value / 1000000ULL;

	// do timezone adjustment
	TimezoneTime* tztime = NULL;
	if (timezone)
	{
		tztime = timezone_match(timezone, time);
		assert(tztime);
		time += tztime->utoff;
	}
	Timestamp ts;
	timestamp_init(&ts);
	ts.us = value % 1000000ULL;
	if (! gmtime_r(&time, &ts.time))
		timestamp_error();

	// timezone offset
	char timezone_sz[16] = { 0 };
	if (timezone)
	{
		int sign   = tztime->utoff >= 0 ? 1: -1;
		int offset = tztime->utoff * sign;
		int hr     =  offset / 3600;
		int min    = (offset - (3600 * hr)) / 60;
		if (min != 0)
			snprintf(timezone_sz, sizeof(timezone_sz), "%c%02d:%02d",
			         sign > 0 ? '+' : '-', hr, min);
		else
			snprintf(timezone_sz, sizeof(timezone_sz), "%c%02d",
			         sign > 0 ? '+' : '-', hr);
	}

	// display in ms or us
	char fraction_sz[16];
	if (ts.us == 0) {
		fraction_sz[0] = 0;
	} else
	if ((ts.us % 1000) == 0)
		snprintf(fraction_sz, sizeof(fraction_sz), ".%03d", ts.us / 1000);
	else
		snprintf(fraction_sz, sizeof(fraction_sz), ".%06d", ts.us);

	int len;
	len = snprintf(str, str_size, "%04d-%02d-%02d %02d:%02d:%02d%s%s",
	               ts.time.tm_year + 1900,
	               ts.time.tm_mon  + 1,
	               ts.time.tm_mday,
	               ts.time.tm_hour,
	               ts.time.tm_min,
	               ts.time.tm_sec,
	               fraction_sz,
	               timezone_sz);
	return len;
}

void
timestamp_add(Timestamp* self, Interval* iv)
{
	// mktime does automatic overflow adjustment

	// years/months
	if (iv->m != 0)
	{
		int m = iv->m;
		int y = m / 12;
		if (y != 0)
		{
			self->time.tm_year += y;
			m = m % 12;
		}
		if (m != 0)
			self->time.tm_mon += m;
	}

	// weeks/days
	if (iv->d != 0)
		self->time.tm_mday += iv->d;

	// hours
	int64_t us = iv->us;
	int64_t hours = us / (60LL * 60 * 1000 * 1000);
	if (hours != 0)
	{
		self->time.tm_hour += hours;
		us = us % (60LL * 60 * 1000 * 1000);
	}

	// minutes
	int64_t minutes = us / (60LL * 1000 * 1000);
	if (minutes != 0)
	{
		self->time.tm_min += minutes;
		us = us % (60LL * 1000 * 1000);
	}

	// seconds
	int64_t seconds = us / (1000LL * 1000);
	if (seconds != 0)
	{
		self->time.tm_sec += seconds;
		us = us % (1000LL * 1000);
	}
	self->us += us;
}

void
timestamp_sub(Timestamp* self, Interval* iv)
{
	// mktime does automatic overflow adjustment

	// years/months
	if (iv->m != 0)
	{
		int m = iv->m;
		int y = m / 12;
		if (y != 0)
		{
			self->time.tm_year -= y;
			m = m % 12;
		}
		if (m != 0)
			self->time.tm_mon -= m;
	}

	// weeks/days
	if (iv->d != 0)
		self->time.tm_mday -= iv->d;

	// hours
	int64_t us = iv->us;
	int64_t hours = us / (60LL * 60 * 1000 * 1000);
	if (hours != 0)
	{
		self->time.tm_hour -= hours;
		us = us % (60LL * 60 * 1000 * 1000);
	}

	// minutes
	int64_t minutes = us / (60LL * 1000 * 1000);
	if (minutes != 0)
	{
		self->time.tm_min -= minutes;
		us = us % (60LL * 1000 * 1000);
	}

	// seconds
	int64_t seconds = us / (1000LL * 1000);
	if (seconds != 0)
	{
		self->time.tm_sec -= seconds;
		us = us % (1000LL * 1000);
	}
	self->us -= us;
}

typedef enum
{
	TIMESTAMP_YEAR,
	TIMESTAMP_MONTH,
	TIMESTAMP_DAY,
	TIMESTAMP_HOUR,
	TIMESTAMP_MINUTE,
	TIMESTAMP_SECOND,
	TIMESTAMP_MILLISECOND,
	TIMESTAMP_MICROSECOND
} TimestampField;

static inline int
timestamp_read_field(Str* type)
{
	switch (*str_of(type)) {
	case 'm':
		// minute
		if (str_is(type, "min", 3) ||
		    str_is(type, "minute", 6))
			return TIMESTAMP_MINUTE;

		// month
		if (str_is(type, "month", 5))
			return TIMESTAMP_MONTH;

		// millisecond
		// milliseconds
		// ms
		if (str_is(type, "ms", 2)            ||
		    str_is(type, "milliseconds", 12) ||
		    str_is(type, "millisecond", 11))
			return TIMESTAMP_MILLISECOND;

		// microseconds
		// microsecond
		if (str_is(type, "microseconds", 12) ||
		    str_is(type, "microsecond", 11))
			return TIMESTAMP_MICROSECOND;
		break;
	case 'u':
		// us
		if (str_is(type, "us", 2))
			return TIMESTAMP_MICROSECOND;
		break;
	case 's':
		// second
		// sec
		if (str_is(type, "sec", 3) ||
		    str_is(type, "second", 6))
			return TIMESTAMP_SECOND;
		break;
	case 'h':
		// hour
		// hr
		if (str_is(type, "hr", 2) ||
		    str_is(type, "hour", 4))
			return TIMESTAMP_HOUR;
		break;
	case 'd':
		// day
		if (str_is(type, "day", 3))
			return TIMESTAMP_DAY;
		break;
	case 'y':
		// year
		if (str_is(type, "year", 4))
			return TIMESTAMP_YEAR;
		break;
	default:
		break;
	}
	return -1;
}

void
timestamp_trunc(Timestamp* self, Str* field)
{
	// timestamp is in utc
	int rc = timestamp_read_field(field);
	if (rc == -1)
		error("unknown timestamp truncate field");

	auto time = &self->time;
	time->tm_wday = -1;
	time->tm_yday = -1;

	switch (rc) {
	case TIMESTAMP_YEAR:
		time->tm_mon = 0;
	case TIMESTAMP_MONTH:
		time->tm_mday = 1;
	case TIMESTAMP_DAY:
		time->tm_hour = 0;
	case TIMESTAMP_HOUR:
		time->tm_min = 0;
	case TIMESTAMP_MINUTE:
		time->tm_sec = 0;
	case TIMESTAMP_SECOND:
		self->us = 0;
	case TIMESTAMP_MILLISECOND:
		self->us = (self->us / 1000) * 1000;
	case TIMESTAMP_MICROSECOND:
		break;
	}
}

int64_t
timestamp_extract(int64_t value, Timezone* timezone, Str* field)
{
	// timestamp is in utc
	int rc = timestamp_read_field(field);
	if (rc == -1)
		error("unknown timestamp field");

	// UTC time in seconds
	time_t time = value / 1000000ULL;

	// do timezone adjustment
	auto tztime = timezone_match(timezone, time);
	assert(tztime);
	time += tztime->utoff;

	Timestamp ts;
	timestamp_init(&ts);
	ts.us = value % 1000000ULL;
	if (! gmtime_r(&time, &ts.time))
		timestamp_error();

	int64_t result;
	auto tm = &ts.time;
	switch (rc) {
	case TIMESTAMP_YEAR:
		result = tm->tm_year + 1900;
		break;
	case TIMESTAMP_MONTH:
		result = tm->tm_mon + 1;
		break;
	case TIMESTAMP_DAY:
		result = tm->tm_mday;
		break;
	case TIMESTAMP_HOUR:
		result = tm->tm_hour;
		break;
	case TIMESTAMP_MINUTE:
		result = tm->tm_min;
		break;
	case TIMESTAMP_SECOND:
		result = tm->tm_sec;
		break;
	case TIMESTAMP_MILLISECOND:
		result = ts.us / 1000;
		break;
	case TIMESTAMP_MICROSECOND:
		result = ts.us;
		break;
	}
	return result;
}

int
timestamp_date(int64_t value)
{
	Timestamp ts;
	timestamp_init(&ts);
	timestamp_set_unixtime(&ts, value);
	return date_set_gregorian(ts.time.tm_year + 1900,
	                          ts.time.tm_mon + 1,
	                          ts.time.tm_mday);
}
