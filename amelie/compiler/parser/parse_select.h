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
typedef struct Plan      Plan;

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
	bool      expr_group_by_has;
	AstList   expr_order_by;
	bool      distinct;
	bool      distinct_on;
	bool      distinct_aggs;
	From      from;
	From      from_group;
	Columns   from_group_columns;
	Target*   pushdown;
	Plan*     plan;
	int       rset_agg;
	int       rset_agg_row;
	int       aggs;
	Program*  program;
};

static inline AstSelect*
ast_select_of(Ast* ast)
{
	return (AstSelect*)ast;
}

static inline AstSelect*
ast_select_allocate(Stmt* stmt, From* outer, Block* block)
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
	self->distinct_aggs     = false;
	self->pushdown          = NULL;
	self->plan              = NULL;
	self->rset_agg          = -1;
	self->rset_agg_row      = -1;
	self->aggs              = -1;
	self->program           = stmt->parser->program;
	returning_init(&self->ret);
	ast_list_init(&self->expr_aggs);
	ast_list_init(&self->expr_group_by);
	ast_list_init(&self->expr_order_by);
	from_init(&self->from, block);
	from_init(&self->from_group, block);
	from_set_outer(&self->from, outer);
	from_set_outer(&self->from_group, outer);
	columns_init(&self->from_group_columns);
	ast_list_add(&stmt->select_list, &self->ast);
	return self;
}

AstSelect* parse_select(Stmt*, From*, bool);
AstSelect* parse_select_expr(Stmt*, Str*);
void       parse_select_resolve(Stmt*);
