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

typedef struct AstWhile AstWhile;

struct AstWhile
{
	Ast    ast;
	Ast*   expr;
	Block* block;
	From   from;
};

static inline AstWhile*
ast_while_of(Ast* ast)
{
	return (AstWhile*)ast;
}

static inline AstWhile*
ast_while_allocate(Block* block)
{
	AstWhile* self;
	self = ast_allocate(0, sizeof(AstWhile));
	self->expr  = NULL;
	self->block = NULL;
	from_init(&self->from, block);
	return self;
}

bool parse_while(Stmt*);
