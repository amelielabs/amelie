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
	Ast*      generated_columns;
	Stmt*     select;
	From      from;
	From      from_generated;
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
	from_init(&self->from_generated, block);
	ast_list_init(&self->rows);
	returning_init(&self->ret);
	return self;
}

void parse_insert(Stmt*);
void parse_generated(Stmt*);
void parse_resolved(Stmt*);
