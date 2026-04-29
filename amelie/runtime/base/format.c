
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

typedef struct Format Format;

struct Format
{
	char* pos;
	char* end;
	char* start;
};

static inline void
format_init(Format* self, char* dest, int size)
{
	self->pos   = dest;
	self->end   = dest + size;
	self->start = dest;
}

hot static inline bool
format_reserve(Format* self, int size)
{
	auto eof = (self->pos + size) >= self->end;
	if (likely(! eof))
		return true;

	// todo: resize if buf, update pos
	return false;
}

hot static inline bool
format_add(Format* self, char* data, int size)
{
	auto eof = (self->pos + size) >= self->end;
	if (likely(! eof))
	{
		memcpy(self->pos, data, size);
		self->pos += size;
		return true;
	}

	// todo: resize if buf, update pos
	size = self->end - self->pos;
	memcpy(self->pos, data, size);
	self->pos = self->end;
	self->end[-1] = 0;
	return false;
}

hot static inline char*
uint64_to_cstr(uint64_t value, char* cstr, int size)
{
	// iterates backwards
	auto at = cstr + size - 1;
	*at = '\0';
	if (value == 0)
	{
		*--at = '0';
		return at;
	}
	while (value > 0)
	{
		*--at = (value % 10) + '0';
		value /= 10;
	}
	return at;
}

hot static inline char*
int64_to_cstr(int64_t value, char* cstr, int size)
{
	// iterates backwards
	uint64_t u64 = (value < 0) ? -value : value;
	auto at = uint64_to_cstr(u64, cstr, size);
	if (value < 0)
		*--at = '-';
	return at;
}

hot static inline bool
format_add_i64(Format* self, int64_t value)
{
	char buf[32];
	auto at = int64_to_cstr(value, buf, sizeof(buf));
	return format_add(self, at, strlen(at));
}

hot static inline bool
format_add_u64(Format* self, uint64_t value)
{
	char buf[32];
	auto at = uint64_to_cstr(value, buf, sizeof(buf));
	return format_add(self, at, strlen(at));
}

hot static inline bool
format_if(const char** pos, const char* data, int size)
{
	for (auto i = 0; i < size; i++)
	  if ((*pos)[i] != data[i])
		  return false;
	*pos += size;
	return true;
}

hot int
format(char* dest, int size, const char* spec, ...)
{
	assert(size > 0);

	va_list args;
	va_start(args, spec);

	Format fmt;
	format_init(&fmt, dest, size);

	auto pos = spec;
	while (*pos)
	{
		if (*pos == '{')
		{
			pos++;
			if (*pos == '{')
			{
				if (unlikely(! format_add(&fmt, "{", 1)))
					break;
				pos++;
			} else
			if (format_if(&pos, "d}", 2))
			{
				auto value = va_arg(args, int);
				if (unlikely(! format_add_i64(&fmt, value)))
					break;
			} else
			if (format_if(&pos, "s}", 2))
			{
				auto value = va_arg(args, char*);
				if (unlikely(! format_add(&fmt, value, strlen(value))))
					break;
			} else
			if (format_if(&pos, "c}", 2))
			{
				char value = va_arg(args, int);
				if (unlikely(! format_add(&fmt, &value, 1)))
					break;
			} else
			if (format_if(&pos, "str}", 4))
			{
				auto value = va_arg(args, Str*);
				if (unlikely(! format_add(&fmt, value->pos, str_size(value))))
					break;
			} else
			if (format_if(&pos, "buf}", 4))
			{
				auto value = va_arg(args, Buf*);
				if (unlikely(! format_add(&fmt, buf_cstr(value), buf_size(value))))
					break;
			} else
			if (format_if(&pos, "i64}", 4))
			{
				auto value = va_arg(args, int64_t);
				if (unlikely(! format_add_i64(&fmt, value)))
					break;
			} else
			if (format_if(&pos, "u64}", 4))
			{
				auto value = va_arg(args, uint64_t);
				if (unlikely(! format_add_u64(&fmt, value)))
					break;
			} else
			if (format_if(&pos, "qs}", 3))
			{
				auto value = va_arg(args, char*);
				if (unlikely(! format_add(&fmt, "\"", 1)))
					break;
				if (unlikely(! format_add(&fmt, value, strlen(value))))
					break;
				if (unlikely(! format_add(&fmt, "\"", 1)))
					break;
			} else
			if (format_if(&pos, "qstr}", 5))
			{
				auto value = va_arg(args, Str*);
				if (unlikely(! format_add(&fmt, "\"", 1)))
					break;
				if (unlikely(! format_add(&fmt, value->pos, str_size(value))))
					break;
				if (unlikely(! format_add(&fmt, "\"", 1)))
					break;
			} else
			{
				// ...}
				auto end = pos;
				while (*end && *end != '}')
					end++;
				if (unlikely(! *end))
				{
					if (unlikely(! format_add(&fmt, (char*)pos, end - pos)))
						break;
					pos = end;
					continue;
				}

				// handle with printf
				auto format_len = end - pos;
				if (unlikely(format_len >= 31))
					break;
				char format[32];
				format[0] = '%';
				memcpy(format + 1, pos, format_len);
				format[1 + format_len] = 0;
				pos = end + 1;

				// calculate output size
				va_list args_copy;
				va_copy(args_copy, args);
				auto len = vsnprintf(NULL, 0, format, args_copy);
				va_end(args_copy);

				// write
				if (unlikely(! format_reserve(&fmt, len)))
					break;
				vsnprintf(fmt.pos, len, format, args);
				fmt.pos += len - 1;
			}
			continue;
		}

		if (*pos == '}')
		{
			pos++;
			if (*pos == '}')
				pos++;
			if (unlikely(! format_add(&fmt, "}", 1)))
				break;
			continue;
		}

		if (*pos == '\\')
		{
			pos++;
			char result;
			switch (*pos) {
			case '\\':
				result = '\\';
				break;
			case '"':
				result = '"';
				break;
			case 'a':
				result = '\a';
				break;
			case 'b':
				result = '\b';
				break;
			case 'f':
				result = '\f';
				break;
			case 'n':
				result = '\n';
				break;
			case 'r':
				result = '\r';
				break;
			case 't':
				result = '\t';
				break;
			case 'v':
				result = '\v';
				break;
			default:
				if (unlikely(! format_add(&fmt, "\\", 1)))
					break;
				continue;
			}
			pos++;
			if (unlikely(! format_add(&fmt, &result, 1)))
				break;
			continue;
		}

		if (unlikely(! format_add(&fmt, (char*)pos, 1)))
			break;
		pos++;
	}
	va_end(args);

	// set \0
	if (unlikely(fmt.pos == fmt.end))
	{
		fmt.pos[-1] = 0;
		fmt.pos--;
	} else {
		fmt.pos[0] = 0;
	}
	return fmt.pos - fmt.start;
}
