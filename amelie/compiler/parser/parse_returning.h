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

typedef struct Returning Returning;

struct Returning
{
	Ast*    list;
	Ast*    list_tail;
	Ast*    list_into;
	Ast*    list_into_tail;
	int     count;
	int     count_into;
	Columns columns;
	Str     format;
	int     r;
	bool    r_recv;
};

static inline void
returning_init(Returning* self)
{
	self->list           = NULL;
	self->list_tail      = NULL;
	self->list_into      = NULL;
	self->list_into_tail = NULL;
	self->list_tail      = NULL;
	self->count          = 0;
	self->count_into     = 0;
	self->r              = -1;
	self->r_recv         = false;
	columns_init(&self->columns);
	str_init(&self->format);
}

static inline void
returning_free(Returning* self)
{
	columns_free(&self->columns);
}

static inline bool
returning_has(Returning* self)
{
	return self->list != NULL;
}

static inline Ast*
returning_find(Returning* self, int order)
{
	// find column starting from 1
	auto pos = 1;
	auto as = self->list;
	while (as) {
		if (pos == order)
			return as;
		as = as->next;
		pos++;
	}
	return NULL;
}

void parse_returning(Returning*, Stmt*, Expr*);
void parse_returning_resolve(Returning*, Stmt*, Targets*);
