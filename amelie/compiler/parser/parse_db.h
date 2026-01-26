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

typedef struct AstDbCreate AstDbCreate;
typedef struct AstDbDrop   AstDbDrop;
typedef struct AstDbAlter  AstDbAlter;

struct AstDbCreate
{
	Ast             ast;
	bool            if_not_exists;
	DatabaseConfig* config;
};

struct AstDbDrop
{
	Ast  ast;
	bool if_exists;
	bool cascade;
	Ast* name;
};

struct AstDbAlter
{
	Ast  ast;
	bool if_exists;
	Ast* name;
	Ast* name_new;
};

static inline AstDbCreate*
ast_db_create_of(Ast* ast)
{
	return (AstDbCreate*)ast;
}

static inline AstDbCreate*
ast_db_create_allocate(void)
{
	AstDbCreate* self;
	self = ast_allocate(0, sizeof(AstDbCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstDbDrop*
ast_db_drop_of(Ast* ast)
{
	return (AstDbDrop*)ast;
}

static inline AstDbDrop*
ast_db_drop_allocate(void)
{
	AstDbDrop* self;
	self = ast_allocate(0, sizeof(AstDbDrop));
	self->if_exists = false;
	self->cascade   = false;
	self->name      = NULL;
	return self;
}

static inline AstDbAlter*
ast_db_alter_of(Ast* ast)
{
	return (AstDbAlter*)ast;
}

static inline AstDbAlter*
ast_db_alter_allocate(void)
{
	AstDbAlter* self;
	self = ast_allocate(0, sizeof(AstDbAlter));
	self->if_exists = false;
	self->name      = NULL;
	self->name_new  = NULL;
	return self;
}

void parse_db_create(Stmt*);
void parse_db_drop(Stmt*);
void parse_db_alter(Stmt*);
