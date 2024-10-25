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

typedef struct AstSchemaCreate AstSchemaCreate;
typedef struct AstSchemaDrop   AstSchemaDrop;
typedef struct AstSchemaAlter  AstSchemaAlter;

struct AstSchemaCreate
{
	Ast           ast;
	bool          if_not_exists;
	SchemaConfig* config;
};

struct AstSchemaDrop
{
	Ast  ast;
	bool if_exists;
	bool cascade;
	Ast* name;
};

struct AstSchemaAlter
{
	Ast  ast;
	bool if_exists;
	Ast* name;
	Ast* name_new;
};

static inline AstSchemaCreate*
ast_schema_create_of(Ast* ast)
{
	return (AstSchemaCreate*)ast;
}

static inline AstSchemaCreate*
ast_schema_create_allocate(void)
{
	AstSchemaCreate* self;
	self = ast_allocate(0, sizeof(AstSchemaCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstSchemaDrop*
ast_schema_drop_of(Ast* ast)
{
	return (AstSchemaDrop*)ast;
}

static inline AstSchemaDrop*
ast_schema_drop_allocate(void)
{
	AstSchemaDrop* self;
	self = ast_allocate(0, sizeof(AstSchemaDrop));
	self->if_exists = false;
	self->cascade   = false;
	self->name      = NULL;
	return self;
}

static inline AstSchemaAlter*
ast_schema_alter_of(Ast* ast)
{
	return (AstSchemaAlter*)ast;
}

static inline AstSchemaAlter*
ast_schema_alter_allocate(void)
{
	AstSchemaAlter* self;
	self = ast_allocate(0, sizeof(AstSchemaAlter));
	self->if_exists = false;
	self->name      = NULL;
	self->name_new  = NULL;
	return self;
}

void parse_schema_create(Stmt*);
void parse_schema_drop(Stmt*);
void parse_schema_alter(Stmt*);
