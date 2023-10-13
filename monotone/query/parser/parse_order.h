#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstOrder AstOrder;

struct AstOrder
{
	Ast  ast;
	Ast* expr;
	int  type;
	bool asc;
};

static inline AstOrder*
ast_order_of(Ast* ast)
{
	return (AstOrder*)ast;
}

static inline AstOrder*
ast_order_allocate(Ast* expr, int type, bool asc)
{
	AstOrder* self;
	self = ast_allocate(0, sizeof(AstOrder));
	self->expr = expr;
	self->type = type;
	self->asc  = asc;
	return self;
}
