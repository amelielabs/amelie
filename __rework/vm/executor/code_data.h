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

typedef struct CodeData CodeData;

struct CodeData
{
	Buf data;
	Buf data_generated;
	Buf call;
};

static inline void
code_data_init(CodeData* self)
{
	buf_init(&self->data);
	buf_init(&self->data_generated);
	buf_init(&self->call);
}

static inline void
code_data_free(CodeData* self)
{
	buf_free(&self->data);
	buf_free(&self->data_generated);
	buf_free(&self->call);
}

static inline void
code_data_reset(CodeData* self)
{
	buf_reset(&self->data);
	buf_reset(&self->data_generated);
	buf_reset(&self->call);
}

static inline void
code_data_copy(CodeData* self, CodeData* from)
{
	buf_write(&self->data, from->data.start, buf_size(&from->data));
	buf_write(&self->call, from->call.start, buf_size(&from->call));
}

static inline int
code_data_offset(CodeData* self)
{
	return buf_size(&self->data);
}

static inline int
code_data_pos(CodeData* self)
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

static inline void*
code_data_at_call(CodeData* self, int id)
{
	assert(self->call.start != NULL);
	auto list = (void**)self->call.start;
	return list[id];
}

static inline int
code_data_count_call(CodeData* self)
{
	return buf_size(&self->call) / sizeof(void*);
}

static inline int
code_data_add_call(CodeData* self, void* pointer)
{
	int id = buf_size(&self->call) / sizeof(void*);
	buf_write(&self->call, &pointer, sizeof(pointer));
	return id;
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
	escape_string(&self->data, string);
	return offset;
}
