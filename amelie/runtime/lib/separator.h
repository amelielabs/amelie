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

typedef struct Separator Separator;

typedef enum
{
	SEPARATOR_EOF,
	SEPARATOR_BEGIN,
	SEPARATOR_IF,
	SEPARATOR_CASE,
	SEPARATOR_FOR,
	SEPARATOR_WHILE,
	SEPARATOR_END,
	SEPARATOR_SEMICOLON
} SeparatorToken;

struct Separator
{
	char* pos;
	char* end;
	Buf   buf;
};

static inline void
separator_init(Separator* self)
{
	self->pos = NULL;
	self->end = NULL;
	buf_init(&self->buf);
}

static inline void
separator_free(Separator* self)
{
	buf_free(&self->buf);
}

static inline void
separator_write(Separator* self, Str* str)
{
	buf_write_str(&self->buf, str);
	buf_write(&self->buf, "\n", 1);
}

static inline bool
separator_pending(Separator* self)
{
	return !buf_empty(&self->buf);
}

hot static inline bool
separator_skip(Separator* self)
{
	// skip white spaces
	while (self->pos < self->end && isspace(*self->pos))
		self->pos++;
	if (self->pos == self->end)
		return true;

	// skip comments --
	if (*self->pos == '-')
	{
		if ((self->end - self->pos) >= 2 && (self->pos[1] == '-'))
		{
			while (self->pos < self->end && *self->pos != '\n')
				self->pos++;
		}
	}
	return self->pos == self->end;
}

static inline bool
separator_skip_pragma(Separator* self)
{
	// skip # .. \n
	while (self->pos < self->end && *self->pos != '\n')
		self->pos++;
	return self->pos == self->end;
}

hot static inline bool
separator_skip_string(Separator* self)
{
	// skip strings
	char end_char = *self->pos;
	self->pos++;

	bool slash = false;
	for (; self->pos < self->end; self->pos++)
	{
		if (*self->pos == end_char) {
			if (slash) {
				slash = false;
				continue;
			}
			self->pos++;
			break;
		}
		if (*self->pos == '\\')
			slash = !slash;
		else
			slash = false;
	}
	return self->pos == self->end;
}

static inline bool
separator_match(Separator* self, char* with, int size)
{
	// <ws | ;> keyword <ws | ;>
	if (self->pos != (char*)self->buf.start && self->pos[-1] != ';' &&
	    !isspace(self->pos[-1]))
		return false;

	if ((self->end - self->pos) < (size + 1))
		return false;
	if (strncasecmp(self->pos, with, size) != 0)
		return false;
	if (self->pos == self->end)
		return false;
	// ws | symbol
	if (!isspace(self->pos[size]) && self->pos[size] != ';')
		return false;
	self->pos += size;
	return true;
}

static inline SeparatorToken
separator_next(Separator* self)
{
	for (;;)
	{
		// skip white spaces and comments
		if (separator_skip(self))
			return SEPARATOR_EOF;

		switch (*self->pos) {
		// BEGIN
		case 'b':
		case 'B':
			if (separator_match(self, "begin", 5))
				return SEPARATOR_BEGIN;
			break;
		// IF
		case 'i':
		case 'I':
			if (separator_match(self, "if", 2))
			{
				if (separator_skip(self))
					return SEPARATOR_EOF;
				// if ;
				if (*self->pos == ';')
					continue;
				// if exists
				// if not
				if (separator_match(self, "exists", 6))
					continue;
				if (separator_match(self, "not", 3))
					continue;

				return SEPARATOR_IF;
			}
			break;
		// CASE
		case 'c':
		case 'C':
			if (separator_match(self, "case", 4))
				return SEPARATOR_CASE;
			break;
		// FOR
		case 'f':
		case 'F':
			if (separator_match(self, "for", 3))
				return SEPARATOR_FOR;
			break;
		// WHILE
		case 'w':
		case 'W':
			if (separator_match(self, "while", 5))
				return SEPARATOR_WHILE;
			break;
		// END
		case 'e':
		case 'E':
			if (separator_match(self, "end", 3))
				return SEPARATOR_END;
			break;
		// ;
		case ';':
			self->pos++;
			return SEPARATOR_SEMICOLON;
		// ' or "
		case '\'':
		case '"':
			if (separator_skip_string(self))
				return SEPARATOR_EOF;
			continue;
		}
		self->pos++;
	}

	return SEPARATOR_EOF;
}

static inline bool
separator_read(Separator* self, Str* block)
{
	//
	// separate incoming data stream into one or more
	// statements by using the ';' symbol as a separator.
	//
	// request for more data, if ';' is not matched.
	//
	// separate BEGIN/IF/CASE/FOR/END statements and return
	// as a single multi-stmt block.
	//
	// request for more data, if END is not matched.
	//
	auto buf = &self->buf;
	if (buf_empty(buf))
		return false;

	// stmt;  [stmt; ...]
	// begin; [stmt; ...] end;
	auto start = buf_cstr(&self->buf);
	self->pos  = start;
	self->end  = start + buf_size(buf);

	// handle empty string
	if (separator_skip(self))
	{
		// empty block
		str_set(block, start, 0);
		return true;
	}

	// handle # pragma
	if (*self->pos == '#')
	{
		separator_skip_pragma(self);
		str_set(block, start, self->pos - start);
		return true;
	}

	int in_case = 0;
	int level = 0;
	for (;;)
	{
		auto rc = separator_next(self);
		switch (rc) {
		case SEPARATOR_BEGIN:
		case SEPARATOR_IF:
		case SEPARATOR_FOR:
		case SEPARATOR_WHILE:
			level++;
			continue;
		case SEPARATOR_CASE:
			// skip case expression
			in_case++;
			continue;
		case SEPARATOR_END:
			if (in_case) {
				in_case--;
				continue;
			}
			level--;
			// error
			if (level < 0)
				break;
			// wait for semicolon on zero level
			continue;
		case SEPARATOR_SEMICOLON:
			if (! level)
				break;
			// wait for end
			continue;
		case SEPARATOR_EOF:
			// request more data
			return false;
		}
		break;
	}

	str_set(block, start, self->pos - start);
	return true;
}

static inline bool
separator_read_leftover(Separator* self, Str* block)
{
	if (! separator_pending(self))
		return false;
	buf_str(&self->buf, block);
	return true;
}

static inline void
separator_advance(Separator* self)
{
	auto start     = buf_cstr(&self->buf);
	auto size      = self->pos - start;
	auto size_left = self->end - self->pos;
	if (size_left > 0)
	{
		memmove(start, self->pos, size_left);
		buf_truncate(&self->buf, size);
	} else
	{
		buf_reset(&self->buf);
	}
}
