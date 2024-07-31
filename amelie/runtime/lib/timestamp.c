
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

always_inline static inline void
timestamp_error(void)
{
	error("malformed timestamp string");
}

hot static inline int
timestamp_parse_part(char* pos, char* pos_end)
{
	int value = 0;
	while (pos < pos_end && isdigit(*pos))
	{
		value = (value * 10) + (*pos - '0');
		pos++;
	}
	if (pos != pos_end)
		timestamp_error();
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
	self->time.tm_year = timestamp_parse_part(pos, pos_end);
	pos += 5;

	// month
	pos_end = pos + 2;
	if (unlikely(*pos_end != '-'))
		goto error;
	self->time.tm_mon = timestamp_parse_part(pos, pos_end);
	pos += 3;

	// day
	pos_end = pos + 2;
	if (unlikely(*pos_end != ' ' && *pos_end != 'T'))
		goto error;
	self->time.tm_mday = timestamp_parse_part(pos, pos_end);
	pos += 3;

	// hour
	pos_end = pos + 2;
	if (unlikely(*pos_end != ':'))
		goto error;
	self->time.tm_hour = timestamp_parse_part(pos, pos_end);
	pos += 3;

	// mm
	pos_end = pos + 2;
	if (unlikely(*pos_end != ':'))
		goto error;
	self->time.tm_min = timestamp_parse_part(pos, pos_end);
	pos += 3;

	// ss
	pos_end = pos + 2;
	self->time.tm_sec = timestamp_parse_part(pos, pos_end);
	pos += 2;

	// [.ssssss]
	if (pos != end && *pos == '.')
	{
		pos++;
		int resolution = 0;
		while (pos < end && isdigit(*pos))
		{
			self->us = (self->us * 10) + (*pos - '0');
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
	int hh = timestamp_parse_part(pos, pos_end);
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
		mm = timestamp_parse_part(pos, pos_end);
	}

	// store timezone offset in seconds
	self->zone = hh * 60 * 60 + mm * 60;
	if (negative)
		self->zone = -self->zone;

	self->zone_set = true;
	return;

error:
	timestamp_error();
}

hot static inline void
timestamp_validate(Timestamp* self)
{
	// validate fields

	// year (starting from unix time)
	if (self->time.tm_year < 1970)
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
	timestamp_error();
}

hot void
timestamp_read(Timestamp* self, Str* str)
{
	// parse iso8601 with microsends
	timestamp_parse(self, str);

	// validate result
	timestamp_validate(self);

	// do corrections to support mktime
	self->time.tm_year -= 1900;
	self->time.tm_mon--;
}

hot void
timestamp_read_value(Timestamp* self, uint64_t value)
{
	// UTC time in seconds
	time_t time = value / 1000000ULL;
	self->us = value % 1000000ULL;
	if (! gmtime_r(&time, &self->time))
		timestamp_error();
}

hot uint64_t
timestamp_of(Timestamp* self, Timezone* timezone)
{
	// mktime
	self->time.tm_isdst = -1;	
	time_t time = mktime(&self->time);
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

	uint64_t value;
	value  = time * 1000000ULL; // convert to us
	value += self->us;
	return value;
}

hot int
timestamp_write(uint64_t value, Timezone* timezone, char* str, int str_size)
{
	// UTC time in seconds
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
	len = snprintf(str, str_size, "%d-%02d-%02d %02d:%02d:%02d%s%s",
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
