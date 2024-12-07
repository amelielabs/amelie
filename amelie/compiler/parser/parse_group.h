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

typedef struct AstGroup AstGroup;

struct AstGroup
{
	Ast     ast;
	int     order;
	Ast*    expr;
	Column* column;
};

static inline AstGroup*
ast_group_of(Ast* ast)
{
	return (AstGroup*)ast;
}

static inline AstGroup*
ast_group_allocate(int order, Ast* expr)
{
	AstGroup* self;
	self = ast_allocate(KAGGRKEY, sizeof(AstGroup));
	self->order  = order;
	self->expr   = expr;
	self->column = NULL;
	return self;
}
