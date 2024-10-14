#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct AstCall AstCall;

struct AstCall
{
	Ast       ast;
	Function* fn;
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
