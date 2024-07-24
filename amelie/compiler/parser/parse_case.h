#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstCase AstCase;

struct AstCase
{
	Ast     ast;
	Ast*    expr;
	Ast*    expr_else;
	AstList when;
};

static inline AstCase*
ast_case_of(Ast* ast)
{
	return (AstCase*)ast;
}

static inline AstCase*
ast_case_allocate(void)
{
	AstCase* self;
	self = ast_allocate(KCASE, sizeof(AstCase));
	self->expr = NULL;
	self->expr_else = NULL;
	ast_list_init(&self->when);
	return self;
}
