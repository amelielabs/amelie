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
	Ast*    expr_limit;
	Ast*    expr_offset;
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
	self = ast_allocate(STMT_INSERT, sizeof(AstUpdate));
	return self;
}

void parse_update(Stmt*);
