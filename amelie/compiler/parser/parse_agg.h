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
	Ast*    name;
	Column* column;
};

static inline AstAgg*
ast_agg_of(Ast* ast)
{
	return (AstAgg*)ast;
}

static inline AstAgg*
ast_agg_allocate(Ast* function, int order, Ast* expr, Ast* expr_init)
{
	AstAgg* self;
	self = ast_allocate(KAGGR, sizeof(AstAgg));
	self->function  = function;
	self->id        = -1;
	self->order     = order;
	self->expr      = expr;
	self->expr_init = expr_init;
	self->name      = NULL;
	self->column    = NULL;
	return self;
}

static inline AstAgg*
ast_agg_match(AstList* list, Str* name)
{
	auto node = list->list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		if (! agg->name)
			continue;
		if (str_compare(&agg->name->string, name))
			return agg;
	}
	return NULL;
}
