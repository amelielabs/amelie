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

typedef struct Cte  Cte;
typedef struct Ctes Ctes;
typedef struct Stmt Stmt;

struct Cte
{
	Str*     name;
	Columns* columns;
	Columns  args;
	Stmt*    stmt;
	Cte*     next;
};

struct Ctes
{
	Cte* list;
	Cte* list_tail;
	int  count;
};

static inline void
ctes_init(Ctes* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline void
ctes_free(Ctes* self)
{
	for (auto cte = self->list; cte; cte = cte->next)
		columns_free(&cte->args);
}

static inline void
ctes_reset(Ctes* self)
{
	ctes_free(self);
	ctes_init(self);
}

static inline Cte*
ctes_find(Ctes* self, Str* name)
{
	for (auto cte = self->list; cte; cte = cte->next)
	{
		if (! cte->name)
			continue;
		if (str_compare(cte->name, name))
			return cte;
	}
	return NULL;
}

static inline Cte*
ctes_add(Ctes* self, Stmt* stmt, Str* name)
{
	auto cte = (Cte*)palloc(sizeof(Cte));
	cte->name    = name;
	cte->stmt    = stmt;
	cte->columns = NULL;
	cte->next    = NULL;
	columns_init(&cte->args);
	if (self->list == NULL)
		self->list = cte;
	else
		self->list_tail->next = cte;
	self->list_tail = cte;
	self->count++;
	return cte;
}
