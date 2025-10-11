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

typedef struct AstUpdate AstUpdate;

struct AstUpdate
{
	Ast       ast;
	Ast*      expr_update;
	Ast*      expr_where;
	Table*    table;
	From      from;
	Returning ret;
	int       rset;
};

static inline AstUpdate*
ast_update_of(Ast* ast)
{
	return (AstUpdate*)ast;
}

static inline AstUpdate*
ast_update_allocate(Block* block)
{
	AstUpdate* self;
	self = ast_allocate(0, sizeof(AstUpdate));
	from_init(&self->from, block);
	returning_init(&self->ret);
	return self;
}

Ast* parse_update_expr(Stmt*);
Ast* parse_update_resolved(Stmt*, Columns*);
void parse_update(Stmt*);
