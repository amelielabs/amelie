#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct AstViewCreate AstViewCreate;
typedef struct AstViewDrop   AstViewDrop;
typedef struct AstViewAlter  AstViewAlter;

struct AstViewCreate
{
	Ast         ast;
	bool        if_not_exists;
	ViewConfig* config;
};

struct AstViewDrop
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
};

struct AstViewAlter
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
	Str  schema_new;
	Str  name_new;
};

static inline AstViewCreate*
ast_view_create_of(Ast* ast)
{
	return (AstViewCreate*)ast;
}

static inline AstViewCreate*
ast_view_create_allocate(void)
{
	AstViewCreate* self;
	self = ast_allocate(0, sizeof(AstViewCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstViewDrop*
ast_view_drop_of(Ast* ast)
{
	return (AstViewDrop*)ast;
}

static inline AstViewDrop*
ast_view_drop_allocate(void)
{
	AstViewDrop* self;
	self = ast_allocate(0, sizeof(AstViewDrop));
	self->if_exists = false;
	str_init(&self->schema);
	str_init(&self->name);
	return self;
}

static inline AstViewAlter*
ast_view_alter_of(Ast* ast)
{
	return (AstViewAlter*)ast;
}

static inline AstViewAlter*
ast_view_alter_allocate(void)
{
	AstViewAlter* self;
	self = ast_allocate(0, sizeof(AstViewAlter));
	self->if_exists = false;
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->schema_new);
	str_init(&self->name_new);
	return self;
}

void parse_view_create(Stmt*);
void parse_view_drop(Stmt*);
void parse_view_alter(Stmt*);
