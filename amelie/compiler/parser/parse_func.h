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

typedef struct AstFunc AstFunc;

struct AstFunc
{
	Ast       ast;
	Function* fn;
};

static inline AstFunc*
ast_func_of(Ast* ast)
{
	return (AstFunc*)ast;
}

static inline AstFunc*
ast_func_allocate(void)
{
	AstFunc* self;
	self = ast_allocate(KFUNC, sizeof(AstFunc));
	self->fn = NULL;
	return self;
}
