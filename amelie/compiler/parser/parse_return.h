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

typedef struct AstReturn AstReturn;

struct AstReturn
{
	Ast      ast;
	Var*     var;
	Columns* columns;
	Str      format;
};

static inline AstReturn*
ast_return_of(Ast* ast)
{
	return (AstReturn*)ast;
}

static inline AstReturn*
ast_return_allocate(void)
{
	AstReturn* self;
	self = ast_allocate(0, sizeof(AstReturn));
	self->var  = NULL;
	str_init(&self->format);
	return self;
}

void parse_return(Stmt*);
