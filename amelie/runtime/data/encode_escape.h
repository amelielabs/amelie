#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

hot static inline void
encode_string_escape(Buf* self, Str* string)
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
	data_write_string32(&start, new_size);
}
