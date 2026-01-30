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

typedef struct AstPartAlter AstPartAlter;

enum
{
	PART_ALTER_REFRESH
};

struct AstPartAlter
{
	Ast      ast;
	uint64_t id;
	Ast*     table;
	int      op;
};

static inline AstPartAlter*
ast_part_alter_of(Ast* ast)
{
	return (AstPartAlter*)ast;
}

static inline AstPartAlter*
ast_part_alter_allocate(void)
{
	AstPartAlter* self;
	self = ast_allocate(0, sizeof(AstPartAlter));
	return self;
}

void parse_part_alter(Stmt*);
