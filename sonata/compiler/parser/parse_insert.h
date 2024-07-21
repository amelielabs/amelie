#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstInsert AstInsert;

enum
{
	ON_CONFLICT_NONE,
	ON_CONFLICT_UPDATE_NONE,
	ON_CONFLICT_UPDATE
};

struct AstInsert
{
	Ast     ast;
	Target* target;
	int     rows;
	int     on_conflict;
	Ast*    update_expr;
	Ast*    update_where;
	Ast*    returning;
};

static inline AstInsert*
ast_insert_of(Ast* ast)
{
	return (AstInsert*)ast;
}

static inline AstInsert*
ast_insert_allocate(void)
{
	AstInsert* self;
	self = ast_allocate(0, sizeof(AstInsert));
	return self;
}

void parse_insert(Stmt*);
