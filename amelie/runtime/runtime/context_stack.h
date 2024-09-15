#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ContextStack ContextStack;

struct ContextStack
{
	char*  pointer;
	size_t size;
	int    valgrind_id;
};

void context_stack_init(ContextStack*);
void context_stack_allocate(ContextStack*, int);
void context_stack_free(ContextStack*);
