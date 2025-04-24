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

typedef struct Op   Op;
typedef struct Code Code;

struct Op
{
	uint8_t op;
	int64_t a, b, c, d;
};

struct Code
{
	Buf code;
	int regs;
};

static inline void
code_init(Code* self)
{
	self->regs = 0;
	buf_init(&self->code);
}

static inline void
code_free(Code* self)
{
	buf_free(&self->code);
}

static inline void
code_reset(Code* self)
{
	self->regs = 0;
	buf_reset(&self->code);
}

static inline void
code_set_regs(Code* self, int regs)
{
	self->regs = regs;
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

hot static inline Op*
code_add(Code*    self,
         uint8_t  id,
         uint64_t a,
         uint64_t b,
         uint64_t c,
         uint64_t d)
{
	buf_reserve(&self->code, sizeof(Op));
	auto op = (Op*)self->code.position;
	op->op    = id;
	op->a     = a;
	op->b     = b;
	op->c     = c;
	op->d     = d;
	buf_advance(&self->code, sizeof(Op));
	return op;
}
