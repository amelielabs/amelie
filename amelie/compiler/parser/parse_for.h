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

typedef struct AstFor AstFor;

struct AstFor
{
	Ast    ast;
	Block* block;
	Buf*   breaks;
	Buf*   continues;
	From   from;
};

static inline AstFor*
ast_for_of(Ast* ast)
{
	return (AstFor*)ast;
}

static inline AstFor*
ast_for_allocate(Block* block)
{
	AstFor* self;
	self = ast_allocate(0, sizeof(AstFor));
	self->block     = NULL;
	self->breaks    = NULL;
	self->continues = NULL;
	from_init(&self->from, block);
	return self;
}

bool parse_for(Stmt*);
