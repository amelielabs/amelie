#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Context Context;

typedef void (*MainFunction)(void*);

struct Context
{
	void** sp;
};

void context_create(Context*, ContextStack*, MainFunction, void*);
void context_swap(Context*, Context*);
