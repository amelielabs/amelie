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

typedef struct Scope  Scope;
typedef struct Scopes Scopes;

struct Scope
{
	Vars   vars;
	Ctes   ctes;
	Scope* next;
};

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
scopes_free(Scopes* self)
{
	for (auto scope = self->list; scope; scope = scope->next)
		ctes_free(&scope->ctes);
}

static inline void
scopes_reset(Scopes* self)
{
	scopes_free(self);
	scopes_init(self);
}

static inline Scope*
scopes_add(Scopes* self)
{
	auto scope = (Scope*)palloc(sizeof(Scope));
	scope->next = NULL;
	vars_init(&scope->vars);
	ctes_init(&scope->ctes);

	if (self->list == NULL)
		self->list = scope;
	else
		self->list_tail->next = scope;
	self->list_tail = scope;
	self->count++;
	return scope;
}
