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

typedef struct Var  Var;
typedef struct Vars Vars;

struct Var
{
	int     order;
	Type    type;
	Columns columns;
	Str*    name;
	Stmt*   writer;
	Var*    next;
};

struct Vars
{
	Var* list;
	Var* list_tail;
	int  count;
};

static inline void
vars_init(Vars* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline void
vars_free(Vars* self)
{
	for (auto var = self->list; var; var = var->next)
		columns_free(&var->columns);
	vars_init(self);
}

static inline Var*
vars_find(Vars* self, Str* name)
{
	for (auto var = self->list; var; var = var->next)
	{
		if (str_compare(var->name, name))
			return var;
	}
	return NULL;
}

static inline Var*
vars_add(Vars* self, Str* name)
{
	auto var = (Var*)palloc(sizeof(Var));
	var->order  = self->count;
	var->type   = TYPE_NULL;
	var->name   = name;
	var->writer = NULL;
	var->next   = NULL;
	if (self->list == NULL)
		self->list = var;
	else
		self->list_tail->next = var;
	self->list_tail = var;
	self->count++;
	columns_init(&var->columns);
	return var;
}
