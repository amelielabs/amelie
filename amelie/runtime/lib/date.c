
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
date_error_str(Str* str)
{
	error("invalid date '%.*s'", str_size(str), str_of(str));
}

hot static inline void
date_validate(int64_t value)
{
	if (unlikely(value < DATE_MIN || value > DATE_MAX))
		error("date overflow");
}

hot static inline int
date_parse_part(Str* str, char* pos, char* pos_end)
{
	int32_t value = 0;
	while (pos < pos_end && isdigit(*pos))
	{
		if (unlikely(int32_mul_add_overflow(&value, value, 10, *pos - '0')))
			date_error_str(str);
		pos++;
	}
	if (pos != pos_end)
		date_error_str(str);
	return value;
}

int
date_set(Str* str)
{
	// yyyy-mm-dd
	auto pos = str->pos;
	auto end = str->end;

	if (unlikely(str_size(str) != 10))
		goto error;

	// year
	auto pos_end = pos + 4;
	if (unlikely(*pos_end != '-'))
		goto error;
	int year = date_parse_part(str, pos, pos_end);
	pos += 5;

	// month
	pos_end = pos + 2;
	if (unlikely(*pos_end != '-'))
		goto error;
	int month = date_parse_part(str, pos, pos_end);
	pos += 3;

	// day
	pos_end = pos + 2;
	if (unlikely(pos_end != end))
		goto error;
	int day = date_parse_part(str, pos, pos_end);

	return date_set_gregorian(year, month, day);

error:
	date_error_str(str);
	return 0;
}

int
date_set_julian(int64_t julian_day)
{
	date_validate(julian_day);
	return julian_day;
}

int
date_get(int64_t julian_day, char* str, int str_size)
{
	date_validate(julian_day);
	int year;
	int month;
	int day;
	date_get_gregorian(julian_day, &year, &month, &day);
	int len;
	len = snprintf(str, str_size, "%04d-%02d-%02d", year, month, day);
	return len;
}

int
date_set_gregorian(int year, int month, int day)
{
    int a = ((14 - month) / 12);
    int y = (year + 4800 - a);
    int m = (month + (12 * a) - 3);
	int julian_day;
	julian_day  = day + (((153 * m) + 2) / 5);
	julian_day += (y * 365) + (y / 4) - (y / 100) + (y / 400) - 32045;
	date_validate(julian_day);
	return julian_day;
}

void
date_get_gregorian(int64_t julian_day, int* year, int* month, int* day)
{
	date_validate(julian_day);
    int a  = julian_day + 32044;
    int b  = (((4 * a) + 3) / 146097);
    int c  = a - ((b * 146097) / 4);
    int d  = (((4 * c) + 3) / 1461);
    int e  = c - ((d * 1461) / 4);
    int f  = (((5 * e) + 2) / 153);
    int g  = (f / 10);
    *year  = ((b * 100) + d - 4800 + g);
    *month = (f + 3 - (12 * g));
    *day   = (e + 1 - (((153 * f) + 2) / 5));
}

int
date_add(int64_t julian_day, int days)
{
	date_validate(julian_day);
	int64_t result = julian_day + days;
	date_validate(result);
	return result;
}

int
date_sub(int64_t julian_day, int days)
{
	date_validate(julian_day);
	int64_t result = julian_day - days;
	date_validate(result);
	return result;
}

int
date_extract(int64_t julian_day, Str* field)
{
	date_validate(julian_day);
	int year;
	int month;
	int day;
	date_get_gregorian(julian_day, &year, &month, &day);

	if (str_is(field, "year", 4))
		return year;

	if (str_is(field, "month", 5))
		return month;

	if (str_is(field, "day", 3))
		return day;

	error("invalid date field '%.*s'", str_size(field),
	      str_of(field));
	return -1;
}
