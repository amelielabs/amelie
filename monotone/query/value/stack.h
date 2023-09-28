#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Stack Stack;

struct Stack
{
	Value lifo[300];
	int   size;
	int   size_max;
};

static inline void
stack_init(Stack* self)
{
	self->size     = 0;
	self->size_max = 300;
	memset(self->lifo, 0, sizeof(self->lifo));
}

always_inline hot static inline int
stack_head(Stack* self)
{
	return self->size;
}

always_inline hot static inline Value*
stack_at(Stack* self, int pos)
{
	return &self->lifo[self->size - pos];
}

always_inline hot static inline Value*
stack_push(Stack* self)
{
	if (unlikely(self->size >= self->size_max))
		error("%s", "stack overflow");
	return &self->lifo[self->size++];
}

always_inline hot static inline Value*
stack_pop(Stack* self)
{
	assert(self->size > 0);
	auto value = &self->lifo[self->size - 1];
	self->size--;
	return value;
}

always_inline hot static inline void
stack_popn(Stack* self, int n)
{
	assert(self->size >= n);
	while (n-- > 0)
	{
		auto value = stack_pop(self);
		value_free(value);
	}
}

static inline void
stack_reset(Stack* self)
{
	if (self->size > 0)
		stack_popn(self, self->size);
}

static inline void
stack_free(Stack* self)
{
	stack_reset(self);
}
