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

typedef struct AstCloneCreate AstCloneCreate;
typedef struct AstCloneDrop   AstCloneDrop;
typedef struct AstCloneAlter  AstCloneAlter;

struct AstCloneCreate
{
	Ast          ast;
	bool         if_not_exists;
	CloneConfig* config;
};

struct AstCloneDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
	bool cascade;
};

enum
{
	CLONE_ALTER_RENAME,
	CLONE_ALTER_DESCRIPTION
};

struct AstCloneAlter
{
	Ast  ast;
	bool if_exists;
	int  type;
	Str  name;
	Str  name_new;
	Str  description;
};

static inline AstCloneCreate*
ast_clone_create_of(Ast* ast)
{
	return (AstCloneCreate*)ast;
}

static inline AstCloneCreate*
ast_clone_create_allocate(void)
{
	AstCloneCreate* self;
	self = ast_allocate(0, sizeof(AstCloneCreate));
	return self;
}

static inline AstCloneDrop*
ast_clone_drop_of(Ast* ast)
{
	return (AstCloneDrop*)ast;
}

static inline AstCloneDrop*
ast_clone_drop_allocate(void)
{
	AstCloneDrop* self;
	self = ast_allocate(0, sizeof(AstCloneDrop));
	return self;
}

static inline AstCloneAlter*
ast_clone_alter_of(Ast* ast)
{
	return (AstCloneAlter*)ast;
}

static inline AstCloneAlter*
ast_clone_alter_allocate(void)
{
	AstCloneAlter* self;
	self = ast_allocate(0, sizeof(AstCloneAlter));
	return self;
}

void parse_clone_create(Stmt*);
void parse_clone_drop(Stmt*);
void parse_clone_alter(Stmt*);
