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

typedef struct AstObjectAlter AstObjectAlter;

enum
{
	OBJECT_ALTER_REFRESH,
	OBJECT_ALTER_SPLIT
};

struct AstObjectAlter
{
	Ast      ast;
	uint64_t id;
	Ast*     table;
	int      type;
	Str      storage;
};

static inline AstObjectAlter*
ast_object_alter_of(Ast* ast)
{
	return (AstObjectAlter*)ast;
}

static inline AstObjectAlter*
ast_object_alter_allocate(void)
{
	AstObjectAlter* self;
	self = ast_allocate(0, sizeof(AstObjectAlter));
	return self;
}

void parse_object_alter(Stmt*);
