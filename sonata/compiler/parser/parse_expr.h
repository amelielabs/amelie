#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
