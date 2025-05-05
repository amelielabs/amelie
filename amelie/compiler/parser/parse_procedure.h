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

typedef struct AstProcedureCreate AstProcedureCreate;
typedef struct AstProcedureDrop   AstProcedureDrop;
typedef struct AstProcedureAlter  AstProcedureAlter;

struct AstProcedureCreate
{
	Ast         ast;
	bool        or_replace;
	ProcConfig* config;
};

struct AstProcedureDrop
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
};

struct AstProcedureAlter
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
	Str  schema_new;
	Str  name_new;
};

static inline AstProcedureCreate*
ast_procedure_create_of(Ast* ast)
{
	return (AstProcedureCreate*)ast;
}

static inline AstProcedureCreate*
ast_procedure_create_allocate(void)
{
	AstProcedureCreate* self;
	self = ast_allocate(0, sizeof(AstProcedureCreate));
	self->or_replace = false;
	self->config     = NULL;
	return self;
}

static inline AstProcedureDrop*
ast_procedure_drop_of(Ast* ast)
{
	return (AstProcedureDrop*)ast;
}

static inline AstProcedureDrop*
ast_procedure_drop_allocate(void)
{
	AstProcedureDrop* self;
	self = ast_allocate(0, sizeof(AstProcedureDrop));
	self->if_exists = false;
	str_init(&self->schema);
	str_init(&self->name);
	return self;
}

static inline AstProcedureAlter*
ast_procedure_alter_of(Ast* ast)
{
	return (AstProcedureAlter*)ast;
}

static inline AstProcedureAlter*
ast_procedure_alter_allocate(void)
{
	AstProcedureAlter* self;
	self = ast_allocate(0, sizeof(AstProcedureAlter));
	self->if_exists = false;
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->schema_new);
	str_init(&self->name_new);
	return self;
}

void parse_procedure_create(Stmt*, bool);
void parse_procedure_drop(Stmt*);
void parse_procedure_alter(Stmt*);
