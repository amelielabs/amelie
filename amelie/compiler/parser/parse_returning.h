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
	int     count;
	Columns columns;
	Str     format;
};

static inline void
returning_init(Returning* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
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

void parse_returning(Returning*, Stmt*, Expr*);
void parse_returning_resolve(Returning*, Stmt*, Target*);
