#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
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
	Ast     ast;
	Target* target;
	int     rows;
	int     on_conflict;
	Ast*    update_expr;
	Ast*    update_where;
	Ast*    returning;
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
	return self;
}

void parse_insert(Stmt*);
