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

enum
{
	SEPARATOR_EOF,
	SEPARATOR_BEGIN,
	SEPARATOR_COMMIT,
	SEPARATOR_MATCH
};

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
	buf_write(&self->buf, "\n", 1);
	buf_write_str(&self->buf, str);
}

static inline bool
separator_pending(Separator* self)
{
	return !buf_empty(&self->buf);
}

static inline bool
separator_match(Separator* self, char* match, int size)
{
	return (self->end - self->pos) >= size && !strncasecmp(self->pos, match, size);
}

static inline bool
separator_next_ws(Separator* self)
{
	// skip white spaces and comments
	while (self->pos < self->end && isspace(*self->pos))
		self->pos++;
	if (self->pos == self->end)
		return true;

	// --
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

static inline int
separator_next(Separator* self)
{
	// skip white spaces and comments
	if (separator_next_ws(self))
		return SEPARATOR_EOF;

	// [EXPLAIN | PROFILE]
	if (*self->pos == 'e' || *self->pos == 'E')
	{
		if (separator_match(self, "explain", 7))
			self->pos += 7;

	} else
	if (*self->pos == 'p' || *self->pos == 'P')
	{
		if (separator_match(self, "profile", 8))
			self->pos += 8;
	}

	// skip white spaces and comments
	if (separator_next_ws(self))
		return SEPARATOR_EOF;

	// [BEGIN | COMMIT]
	int separator = SEPARATOR_MATCH;
	if (*self->pos == 'b' || *self->pos == 'B')
	{
		if (separator_match(self, "begin", 5))
		{
			self->pos += 5;
			separator = SEPARATOR_BEGIN;
		}
		goto eol;
	} else
	if (*self->pos == 'c' || *self->pos == 'C')
	{
		if (separator_match(self, "commit", 6))
		{
			self->pos += 6;
			separator = SEPARATOR_COMMIT;
		}
		goto eol;
	}

eol:
	// match next ;
	while (self->pos < self->end && *self->pos != ';')
		self->pos++;
	if (self->pos == self->end)
		return SEPARATOR_EOF;
	self->pos++;
	return separator;
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
	// separate [EXPLAIN/PROFILE] BEGIN/COMMIT statements and return
	// as a single multi-stmt transaction.
	//
	// request for more data, if COMMIT is not matched.
	//
	auto buf = &self->buf;
	if (buf_empty(buf))
		return false;

	// stmt;  [stmt; ...]
	// begin; [stmt; ...] commit;
	auto start = buf_cstr(&self->buf);
	self->pos  = start;
	self->end  = start + buf_size(buf);

	// handle empty string
	if (separator_next_ws(self))
	{
		str_set(block, start, self->pos - start);
		return true;
	}

	auto begin = false;
	for (auto first = true;; first = false)
	{
		auto rc = separator_next(self);
		switch (rc) {
		// ;
		case SEPARATOR_MATCH:
		{
			// wait for COMMIT
			if (begin)
				continue;
			break;
		}
		// begin
		case SEPARATOR_BEGIN:
		{
			if (begin || !first)
				break;
			begin = true;
			continue;
		}
		// commit
		case SEPARATOR_COMMIT:
			break;
		// eof (request more data)
		case SEPARATOR_EOF:
			return false;
		}
		break;
	}

	str_set(block, start, self->pos - start);
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
