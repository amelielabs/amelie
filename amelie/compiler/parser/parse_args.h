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

typedef struct AstArgs AstArgs;

struct AstArgs
{
	Ast  ast;
	bool constable;
};

static inline AstArgs*
ast_args_of(Ast* ast)
{
	return (AstArgs*)ast;
}

static inline AstArgs*
ast_args_allocate(void)
{
	AstArgs* self;
	self = ast_allocate(KARGS, sizeof(AstArgs));
	return self;
}
