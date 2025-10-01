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
	Ast     ast;
	Block*  block;
	Targets targets;
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
	self->block = NULL;
	targets_init(&self->targets, block);
	return self;
}

bool parse_for(Stmt*);
