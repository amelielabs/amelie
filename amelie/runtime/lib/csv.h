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

typedef struct Csv Csv;

enum
{
	CSV_VALUE,
	CSV_ERROR,
	CSV_EOL,
	CSV_EOF
};

struct Csv
{
	char* pos;
	char* end;
	char  delimiter;
	char  quote;
	Buf   value;
};

static inline void
csv_init(Csv* self)
{
	self->pos       = NULL;
	self->end       = NULL;
	self->delimiter = ',';
	self->quote     = '\"';
	buf_init(&self->value);
}

static inline void
csv_free(Csv* self)
{
	buf_free(&self->value);
}

static inline void
csv_reset(Csv* self)
{
	self->pos       = NULL;
	self->end       = NULL;
	self->delimiter = ',';
	self->quote     = '\"';
	buf_reset(&self->value);
}

static inline void
csv_set(Csv* self, Str* str)
{
	self->pos = str->pos;
	self->end = str->end;
}

hot static inline void
csv_unescape(Csv* self, Str* value)
{
	auto buf = &self->value;
	buf_reset(buf);
	buf_reserve(buf, str_size(value));

	auto pos = value->pos;
	auto end = value->end;
	while (pos < end)
	{
		// \"
		if (*pos == '\\' && (pos + 1 < end) && pos[1] == self->quote)
		{
			buf_append(buf, pos, 2);
			pos += 2;
			continue;
		}

		// "" (write as a single quote)
		if (*pos == self->quote && (pos + 1 < end) && pos[1] == self->quote)
		{
			buf_append(buf, pos, 1);
			pos += 2;
			continue;
		}

		buf_append(buf, pos, 1);
		pos++;
	}

	buf_str(buf, value);
}

hot static inline int
csv_next(Csv* self, Str* value)
{
	// eof
	if (unlikely(self->pos == self->end))
		return CSV_EOF;

	// \r\n
	if (unlikely(*self->pos == '\r'))
	{
		self->pos++;
		if (self->pos != self->end && *self->pos == '\n')
			self->pos++;
		return CSV_EOL;
	}

	// \n
	if (unlikely(*self->pos == '\n'))
	{
		self->pos++;
		return CSV_EOL;
	}

	// next value
	auto start = self->pos;
	if (likely(*start != self->quote))
	{
		// value [, or \r\n or eof]
		while (self->pos < self->end)
		{
			if (*self->pos == self->delimiter ||
			    *self->pos == '\r' ||
				*self->pos == '\n')
				break;
			self->pos++;
		}
		str_set_as(value, start, self->pos);

		// ,
		if (self->pos != self->end && *self->pos == self->delimiter)
			self->pos++;

		return CSV_VALUE;
	}

	// "value"
	auto unescape = false;
	start = ++self->pos;
	while (self->pos < self->end)
	{
		// \"
		if (unlikely(*self->pos == '\\' &&
		             (self->pos + 1 < self->end) &&
		              self->pos[1] == self->quote))
		{
			self->pos += 2;
			continue;
		}

		// "
		if (likely(*self->pos != self->quote))
		{
			self->pos++;
			continue;
		}

		// ""
		if ((self->pos + 1 < self->end) && self->pos[1] == self->quote)
		{
			unescape = true;
			self->pos += 2;
			continue;
		}

		break;
	}

	if (unlikely(self->pos == self->end))
		return CSV_ERROR;

	// "
	str_set_as(value, start, self->pos);
	self->pos++;

	// ,
	if (self->pos != self->end)
	{
		if (*self->pos == self->delimiter)
			self->pos++;
		else
		if (unlikely(*self->pos != '\r' && *self->pos != '\n'))
			return CSV_ERROR;
	}

	// slow path: unescape quotes
	if (unlikely(unescape))
		csv_unescape(self, value);

	return CSV_VALUE;
}
