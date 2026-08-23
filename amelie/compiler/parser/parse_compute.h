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

typedef struct AstComputeCreate AstComputeCreate;
typedef struct AstComputeDrop   AstComputeDrop;
typedef struct AstComputeAlter  AstComputeAlter;

struct AstComputeCreate
{
	Ast            ast;
	bool           if_not_exists;
	ComputeConfig* config;
};

struct AstComputeDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
	bool cascade;
};

enum
{
	COMPUTE_ALTER_RENAME,
	COMPUTE_ALTER_DESCRIPTION
};

struct AstComputeAlter
{
	Ast  ast;
	bool if_exists;
	int  type;
	Str  name;
	Str  name_new;
	Str  description;
};

static inline AstComputeCreate*
ast_compute_create_of(Ast* ast)
{
	return (AstComputeCreate*)ast;
}

static inline AstComputeCreate*
ast_compute_create_allocate(void)
{
	AstComputeCreate* self;
	self = ast_allocate(0, sizeof(AstComputeCreate));
	return self;
}

static inline AstComputeDrop*
ast_compute_drop_of(Ast* ast)
{
	return (AstComputeDrop*)ast;
}

static inline AstComputeDrop*
ast_compute_drop_allocate(void)
{
	AstComputeDrop* self;
	self = ast_allocate(0, sizeof(AstComputeDrop));
	return self;
}

static inline AstComputeAlter*
ast_compute_alter_of(Ast* ast)
{
	return (AstComputeAlter*)ast;
}

static inline AstComputeAlter*
ast_compute_alter_allocate(void)
{
	AstComputeAlter* self;
	self = ast_allocate(0, sizeof(AstComputeAlter));
	return self;
}

void parse_compute_create(Stmt*);
void parse_compute_drop(Stmt*);
void parse_compute_alter(Stmt*);
