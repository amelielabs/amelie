
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

#include <amelie_runtime.h>
#include <valgrind/valgrind.h>

void
context_stack_init(ContextStack* self)
{
	self->size        = 0;
	self->pointer     = NULL;
	self->valgrind_id = 0;
}

void
context_stack_allocate(ContextStack* self, int size)
{
	self->size = size;
	self->pointer = am_malloc(size);
	self->valgrind_id =
		VALGRIND_STACK_REGISTER(self->pointer, self->pointer + size);
}

void
context_stack_free(ContextStack* self)
{
	VALGRIND_STACK_DEREGISTER(self->valgrind_id);
	if (self->pointer)
		am_free(self->pointer);
}
