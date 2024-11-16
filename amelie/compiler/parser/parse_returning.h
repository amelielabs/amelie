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
	Ast* list;
	Ast* list_tail;
	int  count;
};

static inline void
returning_init(Returning* self)
{
	memset(self, 0, sizeof(*self));
}

void parse_returning(Returning*, Stmt*, Expr*);
void parse_returning_resolve(Returning*, Stmt*, Target*);
