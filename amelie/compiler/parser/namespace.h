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

typedef struct Namespace  Namespace;
typedef struct Namespaces Namespaces;

struct Namespace
{
	Vars        vars;
	Blocks      blocks;
	ProcConfig* proc;
	Namespace*  parent;
	Namespace*  next;
};

struct Namespaces
{
	Namespace* list;
	Namespace* list_tail;
	int        count;
};

static inline void
namespaces_init(Namespaces* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline Namespace*
namespaces_add(Namespaces* self, Namespace* parent, ProcConfig* proc)
{
	auto ns = (Namespace*)palloc(sizeof(Namespace));
	ns->proc   = proc;
	ns->parent = parent;
	ns->next   = NULL;
	blocks_init(&ns->blocks, ns);
	vars_init(&ns->vars);

	if (self->list == NULL)
		self->list = ns;
	else
		self->list_tail->next = ns;
	self->list_tail = ns;
	self->count++;
	return ns;
}

static inline Var*
namespace_find_var(Namespace* self, Str* name)
{
	auto var = vars_find(&self->vars, name);
	if (var)
		return var;
	return NULL;
}
