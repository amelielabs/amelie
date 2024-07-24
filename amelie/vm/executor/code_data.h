#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct CodeData CodeData;

struct CodeData
{
	Buf data;
};

static inline void
code_data_init(CodeData* self)
{
	buf_init(&self->data);
}

static inline void
code_data_free(CodeData* self)
{
	buf_free(&self->data);
}

static inline void
code_data_reset(CodeData* self)
{
	buf_reset(&self->data);
}

static inline int
code_data_offset(CodeData* self)
{
	return buf_size(&self->data);
}

static inline uint8_t*
code_data_at(CodeData* self, int offset)
{
	assert(self->data.start != NULL);
	return self->data.start + offset;
}

static inline double
code_data_at_real(CodeData* self, int offset)
{
	assert(self->data.start != NULL);
	uint8_t* pos = self->data.start + offset;
	double value;
	data_read_real(&pos, &value);
	return value;
}

static inline void
code_data_at_string(CodeData* self, int offset, Str* string)
{
	assert(self->data.start != NULL);
	uint8_t* pos = self->data.start + offset;
	data_read_string(&pos, string);
}

static inline int
code_data_pos(CodeData* self)
{
	return buf_size(&self->data);
}

static inline int
code_data_add(CodeData* self, uint8_t* data, int size)
{
	int offset = code_data_pos(self);
	buf_write(&self->data, data, size);
	return offset;
}

static inline int
code_data_add_real(CodeData* self, double real)
{
	int offset = code_data_pos(self);
	encode_real(&self->data, real);
	return offset;
}

static inline int
code_data_add_string(CodeData* self, Str* string)
{
	int offset = code_data_pos(self);
	encode_string(&self->data, string);
	return offset;
}

static inline int
code_data_add_string_unescape(CodeData* self, Str* string)
{
	int offset = code_data_pos(self);
	encode_string32(&self->data, str_size(string));
	buf_reserve(&self->data, str_size(string));

	uint8_t* start  = self->data.position;
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
		if (unlikely(slash)) {
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
		} else {
			*result = *pos;
		}
		result++;
	}

	int new_size = result - start;
	buf_advance(&self->data, new_size);

	// update generated string size
	start = self->data.start + offset;
	data_write_string32(&start, new_size);
	return offset;
}
