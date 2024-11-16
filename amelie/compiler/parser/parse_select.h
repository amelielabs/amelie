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
	Ast       ast;
	Returning ret;
	AstList   expr_aggs;
	Ast*      expr_where;
	Ast*      expr_having;
	Ast*      expr_limit;
	Ast*      expr_offset;
	AstList   expr_group_by;
	AstList   expr_order_by;
	bool      distinct;
	Ast*      distinct_expr;
	Columns   columns;
	Columns   columns_group;
	Target*   target;
	Target*   target_group;
	void*     on_match;
	int       rset;
	int       rmerge;
};

static inline AstSelect*
ast_select_of(Ast* ast)
{
	return (AstSelect*)ast;
}

static inline AstSelect*
ast_select_allocate(void)
{
	AstSelect* self;
	self = ast_allocate(KSELECT, sizeof(AstSelect));
	self->expr_where    = NULL;
	self->expr_having   = NULL;
	self->expr_limit    = NULL;
	self->expr_offset   = NULL;
	self->distinct      = false;
	self->distinct_expr = NULL;
	self->target        = NULL;
	self->target_group  = NULL;
	self->on_match      = NULL;
	self->rset          = -1;
	self->rmerge        = -1;
	returning_init(&self->ret);
	columns_init(&self->columns);
	columns_init(&self->columns_group);
	ast_list_init(&self->expr_aggs);
	ast_list_init(&self->expr_group_by);
	ast_list_init(&self->expr_order_by);
	return self;
}

AstSelect* parse_select(Stmt*);
AstSelect* parse_select_expr(Stmt*);
