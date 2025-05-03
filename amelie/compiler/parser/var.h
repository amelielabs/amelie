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
	int  order;
	int  r;
	Type type;
	Str* name;
	Var* next;
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
	auto var = vars_find(self, name);
	if (var)
		return var;
	var = palloc(sizeof(Var));
	var->order = self->count;
	var->r     = -1;
	var->type  = TYPE_NULL;
	var->name  = name;
	var->next  = NULL;
	if (self->list == NULL)
		self->list = var;
	else
		self->list_tail->next = var;
	self->list_tail = var;
	self->count++;
	return var;
}
