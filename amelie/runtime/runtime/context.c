
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

#if !defined(__amd64) && !defined(__i386)
#error unsupported architecture
#endif

asm (
	"\t.text\n"
	"\t.globl context_swap\n"
	"\t.type context_swap,@function\n"
	"context_swap:\n"
	#if __amd64
	"\tpushq %rbp\n"
	"\tpushq %rbx\n"
	"\tpushq %r12\n"
	"\tpushq %r13\n"
	"\tpushq %r14\n"
	"\tpushq %r15\n"
	"\tmovq %rsp, (%rdi)\n"
	"\tmovq (%rsi), %rsp\n"
	"\tpopq %r15\n"
	"\tpopq %r14\n"
	"\tpopq %r13\n"
	"\tpopq %r12\n"
	"\tpopq %rbx\n"
	"\tpopq %rbp\n"
	"\tret\n"
	#elif __i386
	"\tpushl %ebp\n"
	"\tpushl %ebx\n"
	"\tpushl %esi\n"
	"\tpushl %edi\n"
	"\tmovl %esp, (%eax)\n"
	"\tmovl (%edx), %esp\n"
	"\tpopl %edi\n"
	"\tpopl %esi\n"
	"\tpopl %ebx\n"
	"\tpopl %ebp\n"
	"\tret\n"
	#endif
);

typedef struct
{
	Context*     context_runner;
	Context*     context;
	MainFunction main;
	void*        main_arg;
} ContextRunner;

static __thread ContextRunner runner;

static void
context_entry(void)
{
	// save argument
	volatile ContextRunner runner_arg = runner;

	// return to context_create()
	context_swap(runner_arg.context, runner_arg.context_runner);

	// main
	runner_arg.main(runner_arg.main_arg);

	// unreachable
	abort();
}

static inline void**
context_prepare(ContextStack* stack)
{
	void* *sp = (void**)(stack->pointer + stack->size);
	*--sp = NULL;
	*--sp = (void*)(uintptr_t)context_entry;
#if __amd64
	sp -= 6;
	memset(sp, 0, sizeof(void*) * 6);
#else
	sp -= 4;
	memset(sp, 0, sizeof(void*) * 4);
#endif
	return sp;
}

void
context_create(Context*      self,
               ContextStack* stack,
               MainFunction  main,
               void*         main_arg)
{
	// must be first
	Context context_runner;

	// prepare context runner
	runner.context_runner = &context_runner;
	runner.context        = self;
	runner.main           = main;
	runner.main_arg       = main_arg;

	// prepare context
	self->sp = context_prepare(stack);

	// execute runner: pass function and argument
	context_swap(&context_runner, self);
}
