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

typedef struct AstFunctionCreate AstFunctionCreate;
typedef struct AstFunctionDrop   AstFunctionDrop;
typedef struct AstFunctionAlter  AstFunctionAlter;

struct AstFunctionCreate
{
	Ast        ast;
	bool       or_replace;
	UdfConfig* config;
};

struct AstFunctionDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
	bool cascade;
};

enum
{
	FUNCTION_ALTER_RENAME,
	FUNCTION_ALTER_DESCRIPTION
};

struct AstFunctionAlter
{
	Ast  ast;
	bool if_exists;
	int  type;
	Str  name;
	Str  name_new;
	Str  description;
};

static inline AstFunctionCreate*
ast_function_create_of(Ast* ast)
{
	return (AstFunctionCreate*)ast;
}

static inline AstFunctionCreate*
ast_function_create_allocate(void)
{
	AstFunctionCreate* self;
	self = ast_allocate(0, sizeof(AstFunctionCreate));
	return self;
}

static inline AstFunctionDrop*
ast_function_drop_of(Ast* ast)
{
	return (AstFunctionDrop*)ast;
}

static inline AstFunctionDrop*
ast_function_drop_allocate(void)
{
	AstFunctionDrop* self;
	self = ast_allocate(0, sizeof(AstFunctionDrop));
	return self;
}

static inline AstFunctionAlter*
ast_function_alter_of(Ast* ast)
{
	return (AstFunctionAlter*)ast;
}

static inline AstFunctionAlter*
ast_function_alter_allocate(void)
{
	AstFunctionAlter* self;
	self = ast_allocate(0, sizeof(AstFunctionAlter));
	return self;
}

void parse_function_create(Stmt*, bool);
void parse_function_drop(Stmt*);
void parse_function_alter(Stmt*);
