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

typedef struct AstComputeAlter AstComputeAlter;

struct AstComputeAlter
{
	Ast  ast;
	bool add;
	Ast* id;
};

static inline AstComputeAlter*
ast_compute_alter_of(Ast* ast)
{
	return (AstComputeAlter*)ast;
}

static inline AstComputeAlter*
ast_compute_alter_allocate(void)
{
	AstComputeAlter* self;
	self = ast_allocate(0, sizeof(AstComputeAlter));
	return self;
}

void parse_compute_alter(Stmt*);
