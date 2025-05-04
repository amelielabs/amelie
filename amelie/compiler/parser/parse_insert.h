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
	int       on_conflict;
	Ast*      update_expr;
	Ast*      update_where;
	Ast*      generated_columns;
	Stmt*     select;
	Targets   targets;
	Targets   targets_generated;
	Returning ret;
};

static inline AstInsert*
ast_insert_of(Ast* ast)
{
	return (AstInsert*)ast;
}

static inline AstInsert*
ast_insert_allocate(void)
{
	AstInsert* self;
	self = ast_allocate(0, sizeof(AstInsert));
	targets_init(&self->targets);
	targets_init(&self->targets_generated);
	returning_init(&self->ret);
	return self;
}

void parse_insert(Scope*);
void parse_generated(Scope*);
void parse_resolved(Scope*);
