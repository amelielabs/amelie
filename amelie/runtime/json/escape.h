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

hot static inline void
escape_string(Buf* self, Str* string)
{
	int offset = buf_size(self);
	encode_string32(self, str_size(string));
	buf_reserve(self, str_size(string));

	uint8_t* start  = self->position;
	uint8_t* result = start;
	char*    pos    = str_of(string);
	char*    end    = str_of(string) + str_size(string);

	bool slash = false;
	for (; pos < end; pos++)
	{
		if (*pos == '\\' && !slash) {
			slash = true;
			continue;
		}
		if (likely(! slash)) {
			*result = *pos;
			result++;
			continue;
		}
		switch (*pos) {
		case '\\':
			*result = '\\';
			break;
		case '"':
			*result = '"';
			break;
		case 'a':
			*result = '\a';
			break;
		case 'b':
			*result = '\b';
			break;
		case 'f':
			*result = '\f';
			break;
		case 'n':
			*result = '\n';
			break;
		case 'r':
			*result = '\r';
			break;
		case 't':
			*result = '\t';
			break;
		case 'v':
			*result = '\v';
			break;
		}
		slash = false;
		result++;
	}

	int new_size = result - start;
	buf_advance(self, new_size);

	// update generated string size
	start = self->start + offset;
	json_write_string32(&start, new_size);
}

hot static inline void
escape_string_raw(Buf* self, Str* raw)
{
	auto end = raw->end;
	for (auto pos = raw->pos; pos < end; pos++)
	{
		switch (*pos) {
		case '\"':
			buf_write(self, "\\\"", 2);
			break;
		case '\\':
			buf_write(self, "\\\\", 2);
			break;
		case '\b':
			buf_write(self, "\\b", 2);
			break;
		case '\f':
			buf_write(self, "\\f", 2);
			break;
		case '\n':
			buf_write(self, "\\n", 2);
			break;
		case '\r':
			buf_write(self, "\\r", 2);
			break;
		case '\t':
			buf_write(self, "\\t", 2);
			break;
		default:
			buf_write(self, pos, 1);
			break;
		}
	}
}
