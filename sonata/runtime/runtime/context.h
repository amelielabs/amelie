#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Context Context;

typedef void (*MainFunction)(void*);

struct Context
{
	void** sp;
};

void context_create(Context*, ContextStack*, MainFunction, void*);
void context_swap(Context*, Context*);
