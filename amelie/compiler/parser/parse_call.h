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

struct AstCall
{
	Ast        ast;
	Ast*       args;
	Namespace* ns;
	Proc*      proc;
	From       from;
	Returning  ret;
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
	self->args = NULL;
	self->ns   = NULL;
	self->proc = NULL;
	from_init(&self->from, NULL);
	returning_init(&self->ret);
	return self;
}

bool parse_call(Stmt*);
