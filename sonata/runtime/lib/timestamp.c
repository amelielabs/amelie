
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>

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
	// yyyy-mm-dd< |T>hh:mm:ss[.ssssss][+/-hh|Z|]
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
	if (pos_end != end && *pos_end == '.')
	{
		pos++;
		int resolution = 0;
		while (pos < end && isdigit(*pos))
		{
			self->us = (self->us * 10) + (*pos - '0');
			resolution++;
			pos++;
		}
		if (resolution == 3)
			self->us *= 1000; // convert to us
		else
		if (unlikely(resolution != 6))
			goto error;
	}

	// eof (local time)
	if (pos_end == end)
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
	if (unlikely(pos_end != end))
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
	self->zone = timestamp_parse_part(pos, pos_end);
	// todo: hh::mm
	if (negative)
		self->zone = -self->zone;
	self->zone_set = true;
	return;

error:
	timestamp_error();
}

hot void
timestamp_read(Timestamp* self, Str* str)
{
	// parse iso8601 with microsends
	timestamp_parse(self, str);

	// do corrections to support mktime
	self->time.tm_year -= 1900;
	self->time.tm_mon--;

	// zone
	if (self->zone_set)		
	{
		if (self->zone > 0)
		{
			if (! (self->zone >= 0 && self->zone <= 24))
				goto error;
		} else
		{
			if (self->zone < -24)
				goto error;
		}
	}
	return;
error:
	timestamp_error();
}

hot uint64_t
timestamp_of(Timestamp* self)
{
	// mktime (UTC)
	self->time.tm_isdst = -1;	
	time_t time = mktime(&self->time);
	if (unlikely(time == (time_t)-1))
		timestamp_error();

	uint64_t value;
	value  = time * 1000000ULL; // convert to us
	value += self->us;

	// do timezone adjustment
	if (self->zone_set)
	{
		int zone_adj = self->zone * 60 * 1000000ULL;
		value += zone_adj;
	}

	return value;
}

hot int
timestamp_write(uint64_t value, char* str, int str_size)
{
	time_t time = value / 1000000ULL;
	Timestamp ts;
	timestamp_init(&ts);
	ts.us = value % 1000000ULL;
	if (! gmtime_r(&time, &ts.time))
		timestamp_error();

	int len;
	len = snprintf(str, str_size, "%d-%02d-%02d %02d:%02d:%02d.%06dZ",
	               ts.time.tm_year + 1900,
	               ts.time.tm_mon  + 1,
	               ts.time.tm_mday,
	               ts.time.tm_hour,
	               ts.time.tm_min,
	               ts.time.tm_sec,
	               ts.us);
	return len;
}
