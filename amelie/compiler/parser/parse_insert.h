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

typedef struct AstInsert AstInsert;

enum
{
	ON_CONFLICT_NONE,
	ON_CONFLICT_NOTHING,
	ON_CONFLICT_UPDATE,
	ON_CONFLICT_ERROR
};

struct AstInsert
{
	Ast       ast;
	Set*      values;
	AstList   rows;
	int       on_conflict;
	Ast*      update_expr;
	Ast*      update_where;
	Stmt*     select;
	From      from;
	Returning ret;
};

static inline AstInsert*
ast_insert_of(Ast* ast)
{
	return (AstInsert*)ast;
}

static inline AstInsert*
ast_insert_allocate(Block* block)
{
	AstInsert* self;
	self = ast_allocate(0, sizeof(AstInsert));
	from_init(&self->from, block);
	ast_list_init(&self->rows);
	returning_init(&self->ret);
	return self;
}

void parse_insert(Stmt*);
