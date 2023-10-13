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
	int     order;
	int     aggregate_id;
	Ast*    expr;
	Ast*    label;
	Target* target;
};

static inline AstAggr*
ast_aggr_of(Ast* ast)
{
	return (AstAggr*)ast;
}

static inline AstAggr*
ast_aggr_allocate(int id, int order, Ast* expr, Ast* label)
{
	AstAggr* self;
	self = ast_allocate(id, sizeof(AstAggr));
	self->order        = order;
	self->aggregate_id = 0;
	self->expr         = expr;
	self->label        = label;
	self->target       = NULL;
	return self;
}

static inline AstAggr*
ast_aggr_match(AstList* list, Str* name)
{
	auto node = list->list;
	while (node)
	{
		auto aggr = ast_aggr_of(node->ast);
		if (aggr->label)
		{
			if (str_compare(&aggr->label->string, name))
				return aggr;
		}
		node = node->next;
	}
	return NULL;
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
