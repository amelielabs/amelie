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

typedef struct Call      Call;
typedef struct CallStack CallStack;

struct Call
{
	int stack_head;
	int jmp_ret;
};

struct CallStack
{
	Buf stack;
	int stack_size;
};

static inline void
call_stack_init(CallStack* self)
{
	self->stack_size = 0;
	buf_init(&self->stack);
}

always_inline hot static inline int
call_stack_head(CallStack* self)
{
	return self->stack_size;
}

always_inline hot static inline Call*
call_stack_at(CallStack* self, int pos)
{
	return &((Call*)self->stack.start)[self->stack_size - pos];
}

always_inline hot static inline Call*
call_stack_push(CallStack* self)
{
	self->stack_size++;
	return buf_claim(&self->stack, sizeof(Call));
}

always_inline hot static inline Call*
call_stack_pop(CallStack* self)
{
	assert(self->stack_size > 0);
	auto call = &((Call*)self->stack.start)[self->stack_size - 1];
	buf_truncate(&self->stack, sizeof(Call));
	self->stack_size--;
	return call;
}

always_inline hot static inline void
call_stack_popn(CallStack* self, int n)
{
	assert(self->stack_size >= n);
	while (n-- > 0)
		call_stack_pop(self);
}

static inline void
call_stack_reset(CallStack* self)
{
	if (self->stack_size > 0)
		call_stack_popn(self, self->stack_size);
}

static inline void
call_stack_free(CallStack* self)
{
	call_stack_reset(self);
	buf_free(&self->stack);
}
