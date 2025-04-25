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
	bool     subquery;
	AstList* aggs;
	Ast*     lambda;
	Ast*     as;
	Targets* targets;
};

static inline void
expr_init(Expr* self)
{
	self->select   = false;
	self->subquery = false;
	self->aggs     = NULL;
	self->as       = NULL;
	self->lambda   = NULL;
	self->targets  = NULL;
}

Ast* parse_expr_args(Stmt*, Expr*, int, bool);
Ast* parse_expr(Stmt*, Expr*);
