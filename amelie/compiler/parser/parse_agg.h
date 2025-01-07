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
	Ast*    expr_seed;
	int     expr_seed_type;
	int     rseed;
	bool    distinct;
	Ast*    as;
	Ast*    select;
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
                 Ast* expr_seed,
                 Ast* as)
{
	AstAgg* self;
	self = ast_allocate(KAGGR, sizeof(AstAgg));
	self->function       = function;
	self->id             = -1;
	self->order          = order;
	self->expr           = expr;
	self->expr_seed      = expr_seed;
	self->expr_seed_type = -1;
	self->rseed          = -1;
	self->distinct       = false;
	self->as             = as;
	self->select         = NULL;
	self->column         = NULL;
	return self;
}

static inline bool
ast_agg_has_distinct(AstList* list)
{
	for (auto node = list->list; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		if (agg->distinct)
			return true;
	}
	return false;
}
