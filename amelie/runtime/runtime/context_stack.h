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
