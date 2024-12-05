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

typedef struct Stack Stack;

struct Stack
{
	Buf stack;
	int stack_size;
};

static inline void
stack_init(Stack* self)
{
	self->stack_size = 0;
	buf_init(&self->stack);
}

always_inline hot static inline int
stack_head(Stack* self)
{
	return self->stack_size;
}

always_inline hot static inline Value*
stack_at(Stack* self, int pos)
{
	return &((Value*)self->stack.start)[self->stack_size - pos];
}

always_inline hot static inline Value*
stack_push(Stack* self)
{
	self->stack_size++;
	return buf_claim(&self->stack, sizeof(Value));
}

always_inline hot static inline Value*
stack_pop(Stack* self)
{
	assert(self->stack_size > 0);
	auto value = &((Value*)self->stack.start)[self->stack_size - 1];
	buf_truncate(&self->stack, sizeof(Value));
	self->stack_size--;
	return value;
}

always_inline hot static inline void
stack_popn(Stack* self, int n)
{
	assert(self->stack_size >= n);
	while (n-- > 0)
		value_free(stack_pop(self));
}

static inline void
stack_reset(Stack* self)
{
	if (self->stack_size > 0)
		stack_popn(self, self->stack_size);
}

static inline void
stack_free(Stack* self)
{
	stack_reset(self);
	buf_free(&self->stack);
}
