#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstUpdate AstUpdate;

struct AstUpdate
{
	Ast     ast;
	Table*  table;
	Target* target;
	Ast*    expr_update;
	Ast*    expr_where;
};

static inline AstUpdate*
ast_update_of(Ast* ast)
{
	return (AstUpdate*)ast;
}

static inline AstUpdate*
ast_update_allocate(void)
{
	AstUpdate* self;
	self = ast_allocate(0, sizeof(AstUpdate));
	return self;
}

Ast* parse_update_expr(Stmt*);
void parse_update(Stmt*);
