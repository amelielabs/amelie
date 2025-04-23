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

typedef struct Values Values;

struct Values
{
	Value* set;
	int    set_size;
	Buf    data;
};

static inline void
values_init(Values* self)
{
	self->set      = NULL;
	self->set_size = 0;
	buf_init(&self->data);
}

static inline void
values_reset(Values* self)
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
values_free(Values* self)
{
	values_reset(self);
	buf_free(&self->data);
}

static inline void
values_create(Values* self, int size)
{
	assert(! self->set);
	int allocated = sizeof(Value) * size;
	buf_reserve(&self->data, allocated);
	self->set_size = size;
	self->set      = (Value*)self->data.start;
	memset(self->set, 0, allocated);
}

always_inline hot static inline Value*
values_at(Values* self, int n)
{
	return &self->set[n];
}
