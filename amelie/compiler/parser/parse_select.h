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

typedef struct AstSelect AstSelect;

struct AstSelect
{
	Ast         ast;
	Returning   ret;
	AstList     expr_aggs;
	Ast*        expr_where;
	Ast*        expr_having;
	Ast*        expr_limit;
	Ast*        expr_offset;
	AstList     expr_group_by;
	bool        expr_group_by_has;
	AstList     expr_order_by;
	bool        distinct;
	bool        distinct_on;
	Targets     targets;
	Targets     targets_group;
	Columns     targets_group_columns;
	Target*     pushdown;
	int         rset;
	int         rset_agg;
	int         rset_agg_row;
	int         aggs;
};

static inline AstSelect*
ast_select_of(Ast* ast)
{
	return (AstSelect*)ast;
}

static inline AstSelect*
ast_select_allocate(Targets* outer, Scope* scope)
{
	AstSelect* self;
	self = ast_allocate(KSELECT, sizeof(AstSelect));
	self->expr_where        = NULL;
	self->expr_having       = NULL;
	self->expr_limit        = NULL;
	self->expr_offset       = NULL;
	self->expr_group_by_has = false;
	self->distinct          = false;
	self->distinct_on       = false;
	self->pushdown          = NULL;
	self->rset              = -1;
	self->rset_agg          = -1;
	self->rset_agg_row      = -1;
	self->aggs              = -1;
	returning_init(&self->ret);
	ast_list_init(&self->expr_aggs);
	ast_list_init(&self->expr_group_by);
	ast_list_init(&self->expr_order_by);
	targets_init(&self->targets, scope);
	targets_init(&self->targets_group, scope);
	targets_set_outer(&self->targets, outer);
	targets_set_outer(&self->targets_group, outer);
	columns_init(&self->targets_group_columns);
	return self;
}

AstSelect* parse_select(Stmt*, Targets*, bool);
AstSelect* parse_select_expr(Stmt*);
void       parse_select_resolve(Stmt*);
