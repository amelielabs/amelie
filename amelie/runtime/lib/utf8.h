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

hot static inline bool
utf8_next(char** pos, const char* end)
{
	auto data = (const uint8_t*)*pos;

	// 0000  - 007F     0xxxxxxx
	// 0080  - 07FF     110xxxxx  10xxxxxx
	// 0800  - FFFF     1110xxxx  10xxxxxx 10xxxxxx
	// 10000 - 10FFFF 	11110xxx  10xxxxxx 10xxxxxx 10xxxxxx

	// ascii
	if (likely(data[0] < 0xC0))
	{
		*pos += 1;
		return true;
	}

    if ((data[0] < 0xE0) && (end - *pos) >= 2 &&
	    (data[1] & 0xC0) == 0x80)
	{
		const int wc = ((data[0] & ~0xC0) <<  6) |
		                (data[1] & ~0x80);
		if (wc >= 0x80)
		{
			*pos += 2;
			return true;
		}
	} else
    if ((data[0] < 0xF0) && (end - *pos) >= 3 &&
	    (data[1] & 0xC0) == 0x80 &&
	    (data[2] & 0xC0) == 0x80)
	{
		const int wc = ((data[0] & ~0xE0) << 12) |
		               ((data[1] & ~0x80) << 6)  |
		                (data[2] & ~0x80);
		if (wc >= 0x800)
		{
			*pos += 3;
			return true;
		}
	} else
    if ((data[0] < 0xF8) && (end - *pos) >= 4 &&
	    (data[1] & 0xC0) == 0x80 &&
	    (data[2] & 0xC0) == 0x80 &&
	    (data[3] & 0xC0) == 0x80)
	{
		const int wc = ((data[0] & ~0xF0) << 18) |
		               ((data[1] & ~0x80) << 12) |
		               ((data[2] & ~0x80) << 6)  |
		                (data[3] & ~0x80);
		if (wc >= 0x10000)
		{
			*pos += 4;
			return true;
		}
	}

	// invalid utf8 encoding
	return false;
}

hot static inline size_t
utf8_strlen(Str* self)
{
	// string must have a valid utf8 encoding
	size_t len = 0;
	for (auto pos = self->pos; pos < self->end; pos++)
		if ((*pos & 0xC0) != 0x80)
			len++ ;
	return len;
}
