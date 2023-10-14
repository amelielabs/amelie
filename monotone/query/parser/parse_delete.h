#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstDelete AstDelete;

struct AstDelete
{
	Ast     ast;
	Table*  table;
	Target* target;
	Ast*    expr_where;
	Ast*    expr_limit;
	Ast*    expr_offset;
};

static inline AstDelete*
ast_delete_of(Ast* ast)
{
	return (AstDelete*)ast;
}

static inline AstDelete*
ast_delete_allocate(void)
{
	AstDelete* self;
	self = ast_allocate(STMT_INSERT, sizeof(AstDelete));
	return self;
}

void parse_delete(Stmt*);
