#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstOrder AstOrder;

struct AstOrder
{
	Ast  ast;
	Ast* expr;
	bool asc;
};

static inline AstOrder*
ast_order_of(Ast* ast)
{
	return (AstOrder*)ast;
}

static inline AstOrder*
ast_order_allocate(Ast* expr, bool asc)
{
	AstOrder* self;
	self = ast_allocate(0, sizeof(AstOrder));
	self->expr = expr;
	self->asc  = asc;
	return self;
}
