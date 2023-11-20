#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstAggr AstAggr;

struct AstAggr
{
	Ast     ast;
	int     id;
	int     order;
	Ast*    expr;
	Target* target;
};

static inline AstAggr*
ast_aggr_of(Ast* ast)
{
	return (AstAggr*)ast;
}

static inline AstAggr*
ast_aggr_allocate(int id, int order, Ast* expr)
{
	AstAggr* self;
	self = ast_allocate(KAGGR, sizeof(AstAggr));
	self->id     = id;
	self->order  = order;
	self->expr   = expr;
	self->target = NULL;
	return self;
}

static inline void
ast_aggr_set_target(AstList* list, Target* target)
{
	auto node = list->list;
	while (node)
	{
		auto aggr = ast_aggr_of(node->ast);
		aggr->target = target;
		node = node->next;
	}
}
