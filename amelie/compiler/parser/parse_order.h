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

typedef struct AstOrder AstOrder;

struct AstOrder
{
	Ast  ast;
	Ast* expr;
	int  order;
	bool asc;
};

static inline AstOrder*
ast_order_of(Ast* ast)
{
	return (AstOrder*)ast;
}

static inline AstOrder*
ast_order_allocate(int order, Ast* expr, bool asc)
{
	AstOrder* self;
	self = ast_allocate(0, sizeof(AstOrder));
	self->expr  = expr;
	self->order = order;
	self->asc   = asc;
	return self;
}
