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

typedef struct AstCall AstCall;
typedef struct AstArgs AstArgs;

struct AstCall
{
	Ast       ast;
	Function* fn;
};

struct AstArgs
{
	Ast       ast;
	bool      constable;
};

static inline AstCall*
ast_call_of(Ast* ast)
{
	return (AstCall*)ast;
}

static inline AstCall*
ast_call_allocate(void)
{
	AstCall* self;
	self = ast_allocate(KCALL, sizeof(AstCall));
	self->fn = NULL;
	return self;
}

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
