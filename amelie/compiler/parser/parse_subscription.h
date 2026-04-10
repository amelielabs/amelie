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

typedef struct AstSubCreate AstSubCreate;
typedef struct AstSubDrop   AstSubDrop;

struct AstSubCreate
{
	Ast        ast;
	bool       if_not_exists;
	SubConfig* config;
};

struct AstSubDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

static inline AstSubCreate*
ast_sub_create_of(Ast* ast)
{
	return (AstSubCreate*)ast;
}

static inline AstSubCreate*
ast_sub_create_allocate(void)
{
	AstSubCreate* self;
	self = ast_allocate(0, sizeof(AstSubCreate));
	return self;
}

static inline AstSubDrop*
ast_sub_drop_of(Ast* ast)
{
	return (AstSubDrop*)ast;
}

static inline AstSubDrop*
ast_sub_drop_allocate(void)
{
	AstSubDrop* self;
	self = ast_allocate(0, sizeof(AstSubDrop));
	return self;
}

void parse_sub_create(Stmt*);
void parse_sub_drop(Stmt*);
