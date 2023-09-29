#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Code Code;

struct Code
{
	Buf code;
	Buf data;
};

static inline void
code_init(Code* self)
{
	buf_init(&self->code);
	buf_init(&self->data);
}

static inline void
code_free(Code* self)
{
	buf_free(&self->code);
	buf_free(&self->data);
}

static inline void
code_reset(Code* self)
{
	buf_reset(&self->code);
	buf_reset(&self->data);
}

static inline int
code_count(Code* self)
{
	if (unlikely(self->code.start == NULL))
		return 0;
	return buf_size(&self->code) / sizeof(Op);
}

static inline Op*
code_at(Code* self, int pos)
{
	assert(self->code.start != NULL);
	return &((Op*)self->code.start)[pos];
}

static inline uint8_t*
code_at_data(Code* self, int offset)
{
	assert(self->data.start != NULL);
	return self->data.start + offset;
}

static inline float
code_read_fp(Code* self, int offset)
{
	assert(self->data.start != NULL);
	uint8_t* pos = self->data.start + offset;
	float value;
	data_read_float(&pos, &value);
	return value;
}

static inline void
code_read_string(Code* self, int offset, Str* string)
{
	assert(self->data.start != NULL);
	uint8_t* pos = self->data.start + offset;
	data_read_string(&pos, string);
}

static inline Op*
code_add(Code* self, uint8_t id,
         uint64_t a,
         uint64_t b, uint64_t c, uint64_t d)
{
	buf_reserve(&self->code, sizeof(Op));
	auto op = (Op*)self->code.position;
	op->op    = id;
	op->a     = a;
	op->b     = b;
	op->c     = c;
	op->d     = d;
	op->count = 0;
	buf_advance(&self->code, sizeof(Op));
	return op;
}

static inline int
code_data_prepare(Code* self)
{
	// create array, if there is no data
	int offset;
	offset = buf_size(&self->data);
	if (unlikely(offset == 0))
	{
		encode_array32(&self->data, 0);
		offset = buf_size(&self->data);
	}
	return offset;
}

static inline void
code_data_update(Code* self)
{
	// update array
	int count;
	uint8_t* pos = self->data.start;
	data_read_array(&pos, &count);
	count++;
	pos = self->data.start;
	data_write_array32(&pos, count);
}

static inline int
code_add_data(Code* self, uint8_t* data, int size)
{
	int offset = code_data_prepare(self);
	buf_write(&self->data, data, size);
	code_data_update(self);
	return offset;
}

static inline int
code_add_fp(Code* self, float fp)
{
	int offset = code_data_prepare(self);
	encode_float(&self->data, fp);
	code_data_update(self);
	return offset;
}

static inline int
code_add_string(Code* self, Str* string)
{
	int offset = code_data_prepare(self);
	encode_string(&self->data, string);
	code_data_update(self);
	return offset;
}

static inline int
code_add_string_unescape(Code* self, Str* string)
{
	int offset = code_data_prepare(self);
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

	code_data_update(self);
	return offset;
}

void code_save(Code*, Buf*);
void code_load(Code*, uint8_t**);
void code_dump(Code*, Buf*);
