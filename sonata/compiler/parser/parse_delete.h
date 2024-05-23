#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstDelete AstDelete;

struct AstDelete
{
	Ast     ast;
	Table*  table;
	Target* target;
	Ast*    expr_where;
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
	self = ast_allocate(0, sizeof(AstDelete));
	return self;
}

void parse_delete(Stmt*);
