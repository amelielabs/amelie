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

typedef struct Ref     Ref;
typedef struct Refs    Refs;
typedef struct Targets Targets;

struct Ref
{
	int      order;
	Ast*     ast;
	int      r;
	Targets* targets;
	Ref*     next;
};

struct Refs
{
	Ref* list;
	Ref* list_tail;
	int  count;
};

static inline void
refs_init(Refs* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

hot static inline int
refs_add(Refs* self, Targets* targets, Ast* ast, int r)
{
	if (ast)
	{
		auto ref = self->list;
		while (ref)
		{
			if (ref->ast == ast)
				return ref->order;
			ref = ref->next;
		}
	} else
	{
		assert(r != -1);
		auto ref = self->list;
		while (ref)
		{
			if (ref->r == r)
				return ref->order;
			ref = ref->next;
		}
	}
	auto ref = (Ref*)palloc(sizeof(Ref));
	ref->order   = self->count;
	ref->ast     = ast;
	ref->r       = r;
	ref->targets = targets;
	ref->next    = NULL;

	if (self->list == NULL)
		self->list = ref;
	else
		self->list_tail->next = ref;
	self->list_tail = ref;
	self->count++;
	return ref->order;
}
