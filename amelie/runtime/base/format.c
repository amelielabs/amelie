
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
	Buf*  buf;
};

static inline void
format_init(Format* self, char* dest, int size)
{
	self->pos   = dest;
	self->end   = dest + size;
	self->start = self->pos;
	self->buf   = NULL;
}

static inline void
format_init_buf(Format* self, Buf* buf)
{
	self->pos   = NULL;
	self->end   = NULL;
	self->start = NULL;
	self->buf   = buf;
}

hot static inline bool
format_add(Format* self, char* data, int size)
{
	// write directly to the buf
	if (self->buf)
	{
		buf_write(self->buf, data, size);
		return true;
	}

	// data
	auto eof = (self->pos + size) >= self->end;
	if (likely(! eof))
	{
		memcpy(self->pos, data, size);
		self->pos += size;
		return true;
	}

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
format_add_printf(Format* self, Str* fmt, va_list args)
{
	// handle with printf
	auto fmt_size = str_size(fmt);
	if (unlikely(fmt_size >= 31))
		return false;

	// set format
	char format[32];
	format[0] = '%';
	memcpy(format + 1, fmt->pos, str_size(fmt));
	format[1 + fmt_size] = 0;

	// write directly to buf
	if (self->buf)
	{
		buf_vprintf(self->buf, format, args);
		return true;
	}

	// write
	va_list args_copy;
	va_copy(args_copy, args);
	auto len = vsnprintf(NULL, 0, format, args_copy);
	va_end(args_copy);
	if (likely((self->pos + len) < self->end))
	{
		vsnprintf(self->pos, len, format, args);
		self->pos += len - 1;
		return true;
	}
	return false;
}

hot static inline int
format_finilize(Format* self)
{
	auto buf = self->buf;
	if (buf)
	{
		buf_write(buf, "\0", 1);
		buf_truncate(buf, 1);
		return buf_size(buf);
	}

	// set \0
	if (unlikely(self->pos == self->end))
	{
		self->pos[-1] = 0;
		self->pos--;
	} else {
		self->pos[0] = 0;
	}
	return self->pos - self->start;
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

hot static inline int
format_run(Format* self, const char* spec, va_list args)
{
	auto pos = spec;
	while (*pos)
	{
		// {
		if (*pos == '{')
		{
			pos++;
			if (*pos == '{')
			{
				if (unlikely(! format_add(self, "{", 1)))
					break;
				pos++;
			} else
			if (format_if(&pos, "d}", 2))
			{
				auto value = va_arg(args, int);
				if (unlikely(! format_add_i64(self, value)))
					break;
			} else
			if (format_if(&pos, "s}", 2))
			{
				auto value = va_arg(args, char*);
				if (unlikely(! format_add(self, value, strlen(value))))
					break;
			} else
			if (format_if(&pos, "c}", 2))
			{
				char value = va_arg(args, int);
				if (unlikely(! format_add(self, &value, 1)))
					break;
			} else
			if (format_if(&pos, "str}", 4))
			{
				auto value = va_arg(args, Str*);
				if (unlikely(! format_add(self, value->pos, str_size(value))))
					break;
			} else
			if (format_if(&pos, "buf}", 4))
			{
				auto value = va_arg(args, Buf*);
				if (unlikely(! format_add(self, buf_cstr(value), buf_size(value))))
					break;
			} else
			if (format_if(&pos, "i64}", 4))
			{
				auto value = va_arg(args, int64_t);
				if (unlikely(! format_add_i64(self, value)))
					break;
			} else
			if (format_if(&pos, "u64}", 4))
			{
				auto value = va_arg(args, uint64_t);
				if (unlikely(! format_add_u64(self, value)))
					break;
			} else
			if (format_if(&pos, "qs}", 3))
			{
				auto value = va_arg(args, char*);
				if (unlikely(! format_add(self, "\"", 1)))
					break;
				if (unlikely(! format_add(self, value, strlen(value))))
					break;
				if (unlikely(! format_add(self, "\"", 1)))
					break;
			} else
			if (format_if(&pos, "qstr}", 5))
			{
				auto value = va_arg(args, Str*);
				if (unlikely(! format_add(self, "\"", 1)))
					break;
				if (unlikely(! format_add(self, value->pos, str_size(value))))
					break;
				if (unlikely(! format_add(self, "\"", 1)))
					break;
			} else
			{
				// ...}
				auto end = pos;
				while (*end && *end != '}')
					end++;
				if (unlikely(! *end))
				{
					if (unlikely(! format_add(self, (char*)pos, end - pos)))
						break;
					pos = end;
					continue;
				}

				// handle as printf
				Str fmt;
				str_set(&fmt, (char*)pos, end - pos);
				pos = end + 1;
				if (unlikely(! format_add_printf(self, &fmt, args)))
					break;
			}
			continue;
		}

		// }
		if (*pos == '}')
		{
			pos++;
			if (*pos == '}')
				pos++;
			if (unlikely(! format_add(self, "}", 1)))
				break;
			continue;
		}

		// escape
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
				if (unlikely(! format_add(self, "\\", 1)))
					break;
				continue;
			}
			pos++;
			if (unlikely(! format_add(self, &result, 1)))
				break;
			continue;
		}

		if (unlikely(! format_add(self, (char*)pos, 1)))
			break;
		pos++;
	}
	va_end(args);

	// set \0
	return format_finilize(self);
}

hot int
formatv(char* dest, int size, const char* spec, va_list args)
{
	assert(size > 0);
	Format fmt;
	format_init(&fmt, dest, size);
	return format_run(&fmt, spec, args);
}

hot int
format(char* dest, int size, const char* spec, ...)
{
	assert(size > 0);
	va_list args;
	va_start(args, spec);
	auto rc = formatv(dest, size, spec, args);
	va_end(args);
	return rc;
}

hot int
buf_formatv(Buf* dest, const char* spec, va_list args)
{
	Format fmt;
	format_init_buf(&fmt, dest);
	return format_run(&fmt, spec, args);
}

hot int
buf_format(Buf* dest, const char* spec, ...)
{
	va_list args;
	va_start(args, spec);
	auto rc = buf_formatv(dest, spec, args);
	va_end(args);
	return rc;
}
