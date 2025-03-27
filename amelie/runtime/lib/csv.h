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

struct Csv
{
	char* pos;
	char* end;
	char* start;
};

static inline void
csv_init(Csv* self)
{
	self->pos   = NULL;
	self->end   = NULL;
	self->start = NULL;
}

static inline void
csv_set(Csv* self, Str* text)
{
	self->start = text->pos;
	self->pos   = text->pos;
	self->end   = text->end;
}

/*
	RFC 4180

	file        = record *(CRLF record) [CRLF]
	record      = field *(COMMA field)
	field       = (escaped / non-escaped)
	escaped     = DQUOTE *(TEXTDATA / COMMA / CR / LF / 2DQUOTE) DQUOTE
	non-escaped = *TEXTDATA
	COMMA       = %x2C
	CR          = %x0D
	DQUOTE      = %x22
	LF          = %x0A
	CRLF        = CR LF
	TEXTDATA    = %x20-21 / %x23-2B / %x2D-7E
*/

static inline bool
csv_eof(Csv* self)
{
	return self->pos == self->end;
}

hot static inline Buf*
csv_next_field_quoted(Csv* self)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// "
	self->pos++;
	for (;;)
	{
		// eof
		if (unlikely(self->pos == self->end))
			error("csv: unterminated field");

		if (unlikely(*self->pos == '\"'))
		{
			// ""
			if ((self->pos + 1) != self->end && self->pos[1] == '\"')
			{
				buf_write(buf, self->pos, 1);

				self->pos++;
				self->pos++;
				continue;
			}

			self->pos++;
			break;
		}

		buf_write(buf, self->pos, 1);
		self->pos++;
	}

	// eof
	if (unlikely(self->pos == self->end))
		return buf;

	// ,
	if (*self->pos == ',')
	{
		self->pos++;
		return buf;
	}

	// \n
	if (*self->pos == '\n')
	{
		self->pos++;
		return buf;
	}

	// \r\n
	if (*self->pos == '\r')
	{
		if ((self->pos + 1) == self->end || self->pos[1] != '\n')
			error("csv: invalid crlf marker");

		self->pos++;
		self->pos++;
		return buf;
	}

	error("csv: invalid double-quote field");
	return NULL;
}

hot static inline Buf*
csv_next_field(Csv* self)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	auto start = self->pos;
	auto end   = start;
	for (;;)
	{
		// eof
		if (unlikely(self->pos == self->end))
		{
			end = self->pos;
			break;
		}

		// ,
		if (*self->pos == ',')
		{
			end = self->pos;
			self->pos++;
			break;
		}

		// \n
		if (*self->pos == '\n')
		{
			end = self->pos;
			self->pos++;
			break;
		}

		// \r\n
		if (*self->pos == '\r')
		{
			end = self->pos;
			if ((self->pos + 1) == self->end || self->pos[1] != '\n')
				error("csv: invalid crlf marker");
			self->pos++;
			self->pos++;
			break;
		}
		self->pos++;
	}

	if ((end - start) > 0)
		buf_write(buf, start, end - start);

	return buf;
}

hot static inline Buf*
csv_next(Csv* self)
{
	if (unlikely(self->pos == self->end))
		return NULL;

	// ""
	if (*self->pos == '\"')
		return csv_next_field_quoted(self);

	return csv_next_field(self);
}
