#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
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
