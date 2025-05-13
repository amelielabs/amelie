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

typedef struct AstExecute AstExecute;

struct AstExecute
{
	Ast   ast;
	Proc* proc;
};

static inline AstExecute*
ast_execute_of(Ast* ast)
{
	return (AstExecute*)ast;
}

static inline AstExecute*
ast_execute_allocate(void)
{
	AstExecute* self;
	self = ast_allocate(KCALL, sizeof(AstExecute));
	self->proc = NULL;
	return self;
}

void parse_execute(Stmt*);
