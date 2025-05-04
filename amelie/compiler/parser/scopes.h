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

typedef struct Scopes Scopes;

struct Scopes
{
	Scope* list;
	Scope* list_tail;
	int    count;
};

static inline void
scopes_init(Scopes* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline void
scopes_add(Scopes* self, Scope* scope)
{
	if (self->list == NULL)
		self->list = scope;
	else
		self->list_tail->next = scope;
	self->list_tail = scope;
	self->count++;
}
