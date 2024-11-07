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

typedef struct AstAggr AstAggr;

struct AstAggr
{
	Ast  ast;
	int  id;
	int  order;
	Ast* expr;
	Ast* expr_init;
	Ast* name;
};

static inline AstAggr*
ast_aggr_of(Ast* ast)
{
	return (AstAggr*)ast;
}

static inline AstAggr*
ast_aggr_allocate(int id, int order, Ast* expr, Ast* expr_init)
{
	AstAggr* self;
	self = ast_allocate(KAGGR, sizeof(AstAggr));
	self->id        = id;
	self->order     = order;
	self->expr      = expr;
	self->expr_init = expr_init;
	self->name      = NULL;
	return self;
}

static inline AstAggr*
ast_aggr_match(AstList* list, Str* name)
{
	auto node = list->list;
	for (; node; node = node->next)
	{
		auto aggr = ast_aggr_of(node->ast);
		if (! aggr->name)
			continue;
		if (str_compare(&aggr->name->string, name))
			return aggr;
	}
	return NULL;
}
