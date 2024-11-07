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

typedef struct AstIndexCreate AstIndexCreate;
typedef struct AstIndexDrop   AstIndexDrop;
typedef struct AstIndexAlter  AstIndexAlter;

struct AstIndexCreate
{
	Ast          ast;
	bool         if_not_exists;
	Str          table_schema;
	Str          table_name;
	IndexConfig* config;
};

struct AstIndexDrop
{
	Ast  ast;
	bool if_exists;
	Str  table_schema;
	Str  table_name;
	Str  name;
};

struct AstIndexAlter
{
	Ast  ast;
	bool if_exists;
	Str  table_schema;
	Str  table_name;
	Str  name;
	Str  name_new;
};

static inline AstIndexCreate*
ast_index_create_of(Ast* ast)
{
	return (AstIndexCreate*)ast;
}

static inline AstIndexCreate*
ast_index_create_allocate(void)
{
	AstIndexCreate* self;
	self = ast_allocate(0, sizeof(AstIndexCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	str_init(&self->table_schema);
	str_init(&self->table_name);
	return self;
}

static inline AstIndexDrop*
ast_index_drop_of(Ast* ast)
{
	return (AstIndexDrop*)ast;
}

static inline AstIndexDrop*
ast_index_drop_allocate(void)
{
	AstIndexDrop* self;
	self = ast_allocate(0, sizeof(AstIndexDrop));
	self->if_exists = false;
	str_init(&self->table_schema);
	str_init(&self->table_name);
	str_init(&self->name);
	return self;
}

static inline AstIndexAlter*
ast_index_alter_of(Ast* ast)
{
	return (AstIndexAlter*)ast;
}

static inline AstIndexAlter*
ast_index_alter_allocate(void)
{
	AstIndexAlter* self;
	self = ast_allocate(0, sizeof(AstIndexAlter));
	self->if_exists = false;
	str_init(&self->table_schema);
	str_init(&self->table_name);
	str_init(&self->name);
	str_init(&self->name_new);
	return self;
}

void parse_index_create(Stmt*, bool);
void parse_index_drop(Stmt*);
void parse_index_alter(Stmt*);
