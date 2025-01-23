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

hot static inline int
utf8_sizeof(int byte)
{
    if (likely((byte & 0x80) == 0))
        return 1;
    if ((byte & 0xE0) == 0xC0)
        return 2;
    if ((byte & 0xF0) == 0xE0)
        return 3;
    if ((byte & 0xF8) == 0xF0)
        return 4;
    return -1;
}

hot static inline int
utf8_next(char** pos, const char* end)
{
	auto data = (const uint8_t*)*pos;
	// 0000  - 007F     0xxxxxxx
	// 0080  - 07FF     110xxxxx  10xxxxxx
	// 0800  - FFFF     1110xxxx  10xxxxxx 10xxxxxx
	// 10000 - 10FFFF 	11110xxx  10xxxxxx 10xxxxxx 10xxxxxx
	if (likely((data[0] & 0x80) == 0))
	{
		*pos += 1;
		return data[0];
	}
	int unicode = -1;
	if ((data[0] & 0xE0) == 0xC0 && (end - *pos) >= 2 &&
	    (data[1] & 0xC0) == 0x80)
	{
		unicode = ((data[0] & ~0xC0) <<  6) |
		           (data[1] & ~0x80);
		if (unlikely(unicode < 0x80))
			return -1;
		*pos += 2;
	} else
	if ((data[0] & 0xF0) == 0xE0 && (end - *pos) >= 3 &&
	    (data[1] & 0xC0) == 0x80 &&
	    (data[2] & 0xC0) == 0x80)
	{
		unicode = ((data[0] & ~0xE0) << 12) |
		          ((data[1] & ~0x80) << 6)  |
		           (data[2] & ~0x80);
		if (unlikely(unicode < 0x800))
			return -1;
		*pos += 3;
	} else
	if ((data[0] & 0xF8) == 0xF0 && (end - *pos) >= 4 &&
	    (data[1] & 0xC0) == 0x80 &&
	    (data[2] & 0xC0) == 0x80 &&
	    (data[3] & 0xC0) == 0x80)
	{
		unicode = ((data[0] & ~0xF0) << 18) |
		          ((data[1] & ~0x80) << 12) |
		          ((data[2] & ~0x80) << 6)  |
		           (data[3] & ~0x80);
		if (unicode < 0x10000)
			return -1;
		*pos += 4;
	}
	return unicode;
}

hot static inline void
utf8_forward(char** pos)
{
	*pos += utf8_sizeof(**pos);
}

hot static inline void
utf8_backward(char** pos)
{
	// string must have a valid utf8 encoding
	while ((**(uint8_t**)pos & 0xC0) == 0x80)
		*pos -= 1;
}

hot static inline size_t
utf8_strlen(Str* self)
{
	// string must have a valid utf8 encoding
	size_t len = 0;
	for (auto pos = self->pos; pos < self->end; pos++)
		if ((*(uint8_t*)pos & 0xC0) != 0x80)
			len++;
	return len;
}

hot static inline char*
utf8_at(char* ptr, int pos)
{
	while (pos-- > 0)
		ptr += utf8_sizeof(*ptr);
	return ptr;
}
