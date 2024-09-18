#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AstSelect AstSelect;

struct AstSelect
{
	Ast     ast;
	Ast*    expr;
	int     expr_count;
	AstList expr_aggs;
	AstList expr_labels;
	Ast*    expr_where;
	Ast*    expr_having;
	Ast*    expr_limit;
	Ast*    expr_offset;
	AstList expr_group_by;
	AstList expr_order_by;
	bool    distinct;
	Ast*    distinct_expr;
	int     distinct_expr_count;
	Target* target;
	Target* target_group;
	void*   on_match;
	int     rset;
	int     rgroup;
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
	self->expr                = NULL;
	self->expr_count          = 0;
	self->expr_where          = NULL;
	self->expr_having         = NULL;
	self->expr_limit          = NULL;
	self->expr_offset         = NULL;
	self->distinct            = false;
	self->distinct_expr       = NULL;
	self->distinct_expr_count = 0;
	self->target              = NULL;
	self->target_group        = NULL;
	self->on_match            = NULL;
	self->rset                = -1;
	self->rgroup              = -1;
	ast_list_init(&self->expr_aggs);
	ast_list_init(&self->expr_labels);
	ast_list_init(&self->expr_group_by);
	ast_list_init(&self->expr_order_by);
	return self;
}

AstSelect* parse_select(Stmt*);
AstSelect* parse_select_expr(Stmt*);
