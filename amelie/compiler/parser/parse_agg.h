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

typedef struct AstAgg AstAgg;

struct AstAgg
{
	Ast     ast;
	int     id;
	int     order;
	Ast*    function;
	Ast*    expr;
	Ast*    expr_init;
	Ast*    as;
	Column* column;
};

static inline AstAgg*
ast_agg_of(Ast* ast)
{
	return (AstAgg*)ast;
}

static inline AstAgg*
ast_agg_allocate(Ast* function, int order,
                 Ast* expr,
                 Ast* expr_init,
                 Ast* as)
{
	AstAgg* self;
	self = ast_allocate(KAGGR, sizeof(AstAgg));
	self->function  = function;
	self->id        = -1;
	self->order     = order;
	self->expr      = expr;
	self->expr_init = expr_init;
	self->as        = as;
	self->column    = NULL;
	return self;
}
