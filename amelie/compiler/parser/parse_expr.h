#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Expr Expr;

struct Expr
{
	AstList* aggs;
};

static inline void
expr_init(Expr* self)
{
	self->aggs = NULL;
}

Ast* parse_expr(Stmt*, Expr*);
