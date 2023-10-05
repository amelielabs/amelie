#pragma once

//
// monotone
//
// SQL OLTP database
//

hot static inline int
rpin(Compiler* self)
{
	return rmap_pin(&self->map);
}

static inline void
runpin(Compiler* self, int r)
{
	rmap_unpin(&self->map, r);
}

static inline void
op0(Compiler* self, uint8_t id)
{
	code_add(self->code, id, 0, 0, 0, 0);
}

static inline int64_t
op1(Compiler* self, uint8_t id, int64_t a)
{
	code_add(self->code, id, a, 0, 0, 0);
	return a;
}

static inline int64_t
op2(Compiler* self, uint8_t id, int64_t a, int64_t b)
{
	code_add(self->code, id, a, b, 0, 0);
	return a;
}

static inline int64_t
op3(Compiler* self, uint8_t id, int64_t a, int64_t b, int64_t c)
{
	code_add(self->code, id, a, b, c, 0);
	return a;
}

static inline int64_t
op4(Compiler* self, uint8_t id, int64_t a, int64_t b, int64_t c, int64_t d)
{
	code_add(self->code, id, a, b, c, d);
	return a;
}

static inline int
op_pos(Compiler* self)
{
	return code_count(self->code);
}

static inline void
op_set_jmp(Compiler* self, int pos, int to)
{
	code_at(self->code, pos)->a = to;
}
