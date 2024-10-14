#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Reg Reg;

struct Reg
{
	Value r[64];
};

static inline void
reg_init(Reg* self)
{
	memset(self->r, 0, sizeof(self->r));
}

static inline void
reg_reset(Reg* self)
{
	int i = 0;
	for (; i < 64; i++)
		value_free(&self->r[i]);
}

always_inline hot static inline Value*
reg_at(Reg* self, int n)
{
	return &self->r[n];
}
