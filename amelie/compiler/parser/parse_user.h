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
	bool cascade;
	Ast* name;
};

enum
{
	USER_ALTER_RENAME,
	USER_ALTER_REVOKE
};

struct AstUserAlter
{
	Ast  ast;
	bool if_exists;
	int  type;
	Ast* name;
	Ast* name_new;
	Str  revoked_at;
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
	return self;
}

void parse_user_create(Stmt*, bool);
void parse_user_drop(Stmt*);
void parse_user_alter(Stmt*);
