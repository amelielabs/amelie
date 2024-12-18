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

typedef struct Expr Expr;

struct Expr
{
	bool     select;
	AstList* aggs;
	Ast*     lambda;
	Ast*     as;
};

static inline void
expr_init(Expr* self)
{
	self->select = false;
	self->aggs   = NULL;
	self->as     = NULL;
	self->lambda = NULL;
}

Ast* parse_expr_args(Stmt*, Expr*, int, bool);
Ast* parse_expr(Stmt*, Expr*);
