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

typedef struct Reg Reg;

struct Reg
{
	Value* r;
	int    r_count;
	Buf    data;
};

always_inline hot static inline Value*
reg_at(Reg* self, int n)
{
	return &self->r[n];
}

static inline void
reg_init(Reg* self)
{
	self->r       = NULL;
	self->r_count = 0;
	buf_init(&self->data);
}

static inline void
reg_reset(Reg* self)
{
	for (int i = 0; i < self->r_count; i++)
		value_free(&self->r[i]);
	self->r       = NULL;
	self->r_count = 0;
	buf_reset(&self->data);
}

static inline void
reg_free(Reg* self)
{
	reg_reset(self);
	buf_free(&self->data);
}

static inline void
reg_prepare(Reg* self, int count)
{
	assert(! self->r);
	int allocated = sizeof(Value) * count;
	buf_reserve(&self->data, allocated);
	self->r_count = count;
	self->r       = (Value*)self->data.start;
	memset(self->r, 0, allocated);
}
