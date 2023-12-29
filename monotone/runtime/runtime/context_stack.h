#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct ContextStack ContextStack;

struct ContextStack
{
	char*  pointer;
	size_t size;
	int    valgrind_id;
};

void context_stack_init(ContextStack*);
int  context_stack_allocate(ContextStack*, int);
void context_stack_free(ContextStack*);
