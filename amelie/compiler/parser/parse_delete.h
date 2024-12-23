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

typedef struct AstDelete AstDelete;

struct AstDelete
{
	Ast       ast;
	Ast*      expr_where;
	Table*    table;
	Targets   targets;
	Returning ret;
	int       rset;
};

static inline AstDelete*
ast_delete_of(Ast* ast)
{
	return (AstDelete*)ast;
}

static inline AstDelete*
ast_delete_allocate(void)
{
	AstDelete* self;
	self = ast_allocate(0, sizeof(AstDelete));
	returning_init(&self->ret);
	return self;
}

void parse_delete(Stmt*);
