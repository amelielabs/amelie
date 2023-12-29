#pragma once

//
// indigo
//
// SQL OLTP database
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
