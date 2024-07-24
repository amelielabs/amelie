
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <valgrind/valgrind.h>

void
context_stack_init(ContextStack* self)
{
	self->size        = 0;
	self->pointer     = 0;
	self->valgrind_id = 0;
}

int
context_stack_allocate(ContextStack* self, int size)
{
	self->size = size;
	self->pointer = am_malloc_nothrow(size);
	if (unlikely(self->pointer == NULL))
		return -1;
	self->valgrind_id =
		VALGRIND_STACK_REGISTER(self->pointer, self->pointer + size);
	return 0;
}

void
context_stack_free(ContextStack* self)
{
	VALGRIND_STACK_DEREGISTER(self->valgrind_id);
	if (self->pointer)
		am_free(self->pointer);
}
