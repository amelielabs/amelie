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

typedef struct AstBranchCreate AstBranchCreate;
typedef struct AstBranchDrop   AstBranchDrop;
typedef struct AstBranchAlter  AstBranchAlter;

struct AstBranchCreate
{
	Ast           ast;
	bool          if_not_exists;
	BranchConfig* config;
};

struct AstBranchDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

struct AstBranchAlter
{
	Ast  ast;
	bool if_exists;
	Str  name;
	Str  name_new;
};

static inline AstBranchCreate*
ast_branch_create_of(Ast* ast)
{
	return (AstBranchCreate*)ast;
}

static inline AstBranchCreate*
ast_branch_create_allocate(void)
{
	AstBranchCreate* self;
	self = ast_allocate(0, sizeof(AstBranchCreate));
	return self;
}

static inline AstBranchDrop*
ast_branch_drop_of(Ast* ast)
{
	return (AstBranchDrop*)ast;
}

static inline AstBranchDrop*
ast_branch_drop_allocate(void)
{
	AstBranchDrop* self;
	self = ast_allocate(0, sizeof(AstBranchDrop));
	return self;
}

static inline AstBranchAlter*
ast_branch_alter_of(Ast* ast)
{
	return (AstBranchAlter*)ast;
}

static inline AstBranchAlter*
ast_branch_alter_allocate(void)
{
	AstBranchAlter* self;
	self = ast_allocate(0, sizeof(AstBranchAlter));
	return self;
}

void parse_branch_create(Stmt*);
void parse_branch_drop(Stmt*);
void parse_branch_alter(Stmt*);
