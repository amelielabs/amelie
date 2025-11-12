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
	bool     udf;
	bool     and_or;
	AstList* aggs;
	AstList* names;
	Ast*     lambda;
	Ast*     as;
	From*    from;
};

static inline void
expr_init(Expr* self)
{
	self->select   = false;
	self->subquery = false;
	self->udf      = false;
	self->and_or   = true;
	self->aggs     = NULL;
	self->names    = NULL;
	self->as       = NULL;
	self->lambda   = NULL;
	self->from     = NULL;
}

bool parse_expr_is_const(Ast*);
Ast* parse_expr_args(Stmt*, Expr*, int, bool);
Ast* parse_expr(Stmt*, Expr*);
