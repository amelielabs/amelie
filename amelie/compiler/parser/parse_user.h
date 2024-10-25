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

typedef struct AstUserCreate AstUserCreate;
typedef struct AstUserDrop   AstUserDrop;
typedef struct AstUserAlter  AstUserAlter;

struct AstUserCreate
{
	Ast         ast;
	bool        if_not_exists;
	UserConfig* config;
};

struct AstUserDrop
{
	Ast  ast;
	bool if_exists;
	Ast* name;
};

struct AstUserAlter
{
	Ast         ast;
	UserConfig* config;
};

static inline AstUserCreate*
ast_user_create_of(Ast* ast)
{
	return (AstUserCreate*)ast;
}

static inline AstUserCreate*
ast_user_create_allocate(void)
{
	AstUserCreate* self;
	self = ast_allocate(0, sizeof(AstUserCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstUserDrop*
ast_user_drop_of(Ast* ast)
{
	return (AstUserDrop*)ast;
}

static inline AstUserDrop*
ast_user_drop_allocate(void)
{
	AstUserDrop* self;
	self = ast_allocate(0, sizeof(AstUserDrop));
	self->if_exists = false;
	self->name      = NULL;
	return self;
}

static inline AstUserAlter*
ast_user_alter_of(Ast* ast)
{
	return (AstUserAlter*)ast;
}

static inline AstUserAlter*
ast_user_alter_allocate(void)
{
	AstUserAlter* self;
	self = ast_allocate(0, sizeof(AstUserAlter));
	self->config = NULL;
	return self;
}

void parse_user_create(Stmt*);
void parse_user_drop(Stmt*);
void parse_user_alter(Stmt*);
