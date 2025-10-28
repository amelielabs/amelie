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

typedef struct AstBreak AstBreak;

struct AstBreak
{
	Ast ast;
};

static inline AstBreak*
ast_break_of(Ast* ast)
{
	return (AstBreak*)ast;
}

static inline AstBreak*
ast_break_allocate(void)
{
	AstBreak* self;
	self = ast_allocate(KBREAK, sizeof(AstBreak));
	return self;
}

void parse_break(Stmt*);
