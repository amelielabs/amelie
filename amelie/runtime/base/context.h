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

typedef struct Context Context;

typedef void (*MainFunction)(void*);

struct Context
{
	void** sp;
};

void context_create(Context*, ContextStack*, MainFunction, void*);
void context_swap(Context*, Context*);
