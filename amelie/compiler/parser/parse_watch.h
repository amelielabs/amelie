#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AstWatch AstWatch;

struct AstWatch
{
	Ast  ast;
	Ast* expr;
};

static inline AstWatch*
ast_watch_of(Ast* ast)
{
	return (AstWatch*)ast;
}

static inline AstWatch*
ast_watch_allocate(void)
{
	AstWatch* self = ast_allocate(0, sizeof(AstWatch));
	self->expr = NULL;
	return self;
}

void parse_watch(Stmt*);
