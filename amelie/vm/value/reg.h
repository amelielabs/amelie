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
};

static inline void
reg_init(Reg* self)
{
	self->r = NULL;
}

static inline void
reg_reset(Reg* self)
{
	if (! self->r)
		return;
	for (int i = 0; i < 64; i++)
		value_free(&self->r[i]);
}

static inline void
reg_free(Reg* self)
{
	am_free(self->r);
}

static inline void
reg_prepare(Reg* self)
{
	if (self->r)
		return;
	int size = sizeof(Value) * 64;
	self->r = am_malloc(size);
	memset(self->r, 0, size);
}

always_inline hot static inline Value*
reg_at(Reg* self, int n)
{
	return &self->r[n];
}
