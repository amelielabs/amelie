#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Result Result;

struct Result
{
	Value* set;
	int    set_size;
	Buf    data;
};

static inline void
result_init(Result* self)
{
	self->set      = NULL;
	self->set_size = 0;
	buf_init(&self->data);
}

static inline void
result_reset(Result* self)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto value = &self->set[i];
		value_free(value);
	}
	self->set      = NULL;
	self->set_size = 0;
	buf_reset(&self->data);
}

static inline void
result_free(Result* self)
{
	result_reset(self);
	buf_free(&self->data);
}

static inline void
result_create(Result* self, int size)
{
	assert(! self->set);
	int allocated = sizeof(Value) * size;
	buf_reserve(&self->data, allocated);
	self->set_size = size;
	self->set      = (Value*)self->data.start;
	memset(self->set, 0, allocated);
}

always_inline hot static inline Value*
result_at(Result* self, int n)
{
	return &self->set[n];
}
