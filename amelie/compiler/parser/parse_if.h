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

typedef struct AstIfCond AstIfCond;
typedef struct AstIf     AstIf;

struct AstIfCond
{
	Ast    ast;
	int    order;
	Ast*   expr;
	Block* block;
};

struct AstIf
{
	Ast     ast;
	AstList conds;
	Block*  cond_else;
	From    from;
};

static inline AstIfCond*
ast_if_cond_of(Ast* ast)
{
	return (AstIfCond*)ast;
}

static inline AstIfCond*
ast_if_cond_allocate(void)
{
	AstIfCond* self;
	self = ast_allocate(0, sizeof(AstIfCond));
	self->order = 0;
	self->expr  = NULL;
	self->block = NULL;
	return self;
}

static inline AstIf*
ast_if_of(Ast* ast)
{
	return (AstIf*)ast;
}

static inline AstIf*
ast_if_allocate(Block* block)
{
	AstIf* self;
	self = ast_allocate(0, sizeof(AstIf));
	self->cond_else = NULL;
	ast_list_init(&self->conds);
	from_init(&self->from, block);
	return self;
}

bool parse_if(Stmt*);
