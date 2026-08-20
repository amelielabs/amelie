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

typedef struct AstSystemAlter  AstSystemAlter;

enum
{
	SYSTEM_ALTER_SECRET_ROTATE,
	SYSTEM_ALTER_SET_CDC,
	SYSTEM_ALTER_UNSET_CDC
};

struct AstSystemAlter
{
	Ast      ast;
	int      type;
	uint64_t cdc_limit;
};

static inline AstSystemAlter*
ast_system_alter_of(Ast* ast)
{
	return (AstSystemAlter*)ast;
}

static inline AstSystemAlter*
ast_system_alter_allocate(void)
{
	AstSystemAlter* self;
	self = ast_allocate(0, sizeof(AstSystemAlter));
	return self;
}

void parse_system_alter(Stmt*);
