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
