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
	bool    replace;
	Target* target;
	int     rows;
	int     on_conflict;
	Ast*    update_expr;
	Ast*    update_where;
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
	self->replace      = false;
	self->target       = NULL;
	self->rows         = 0;
	self->on_conflict  = ON_CONFLICT_NONE;
	self->update_expr  = NULL;
	self->update_where = NULL;
	return self;
}

void parse_insert(Stmt*, bool);
