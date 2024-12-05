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
	Buf data_call;
};

static inline void
code_data_init(CodeData* self)
{
	buf_init(&self->data);
	buf_init(&self->data_call);
}

static inline void
code_data_free(CodeData* self)
{
	buf_free(&self->data);
	buf_free(&self->data_call);
}

static inline void
code_data_reset(CodeData* self)
{
	buf_reset(&self->data);
	buf_reset(&self->data_call);
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
code_data_at_double(CodeData* self, int offset)
{
	assert(self->data.start != NULL);
	return *(double*)(self->data.start + offset);
}

static inline void
code_data_at_string(CodeData* self, int offset, Str* string)
{
	assert(self->data.start != NULL);
	uint8_t* pos = self->data.start + offset;
	json_read_string(&pos, string);
}

static inline void*
code_data_at_call(CodeData* self, int id)
{
	assert(self->data_call.start != NULL);
	auto list = (void**)self->data_call.start;
	return list[id];
}

static inline int
code_data_count_call(CodeData* self)
{
	return buf_size(&self->data_call) / sizeof(void*);
}

static inline int
code_data_add_call(CodeData* self, void* pointer)
{
	int id = buf_size(&self->data_call) / sizeof(void*);
	buf_write(&self->data_call, &pointer, sizeof(pointer));
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
code_data_add_double(CodeData* self, double value)
{
	int offset = code_data_pos(self);
	buf_write_double(&self->data, value);
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
