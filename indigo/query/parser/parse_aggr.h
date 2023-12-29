#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct AstAggr AstAggr;

struct AstAggr
{
	Ast  ast;
	int  id;
	int  order;
	Ast* expr;
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
	self->id    = id;
	self->order = order;
	self->expr  = expr;
	return self;
}
