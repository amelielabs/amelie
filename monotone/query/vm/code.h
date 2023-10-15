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
};

static inline void
code_init(Code* self)
{
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
	buf_reset(&self->code);
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

static inline Op*
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
	op->count = 0;
	buf_advance(&self->code, sizeof(Op));
	return op;
}

void code_save(Code*, CodeData*, Buf*);
void code_load(Code*, CodeData*, uint8_t**);
void code_dump(Code*, CodeData*, Buf*);
