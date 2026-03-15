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

typedef struct AstSynonymCreate AstSynonymCreate;
typedef struct AstSynonymDrop   AstSynonymDrop;
typedef struct AstSynonymAlter  AstSynonymAlter;

struct AstSynonymCreate
{
	Ast            ast;
	bool           if_not_exists;
	SynonymConfig* config;
};

struct AstSynonymDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

struct AstSynonymAlter
{
	Ast  ast;
	bool if_exists;
	Str  name;
	Str  name_new;
};

static inline AstSynonymCreate*
ast_synonym_create_of(Ast* ast)
{
	return (AstSynonymCreate*)ast;
}

static inline AstSynonymCreate*
ast_synonym_create_allocate(void)
{
	AstSynonymCreate* self;
	self = ast_allocate(0, sizeof(AstSynonymCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstSynonymDrop*
ast_synonym_drop_of(Ast* ast)
{
	return (AstSynonymDrop*)ast;
}

static inline AstSynonymDrop*
ast_synonym_drop_allocate(void)
{
	AstSynonymDrop* self;
	self = ast_allocate(0, sizeof(AstSynonymDrop));
	self->if_exists = false;
	str_init(&self->name);
	return self;
}

static inline AstSynonymAlter*
ast_synonym_alter_of(Ast* ast)
{
	return (AstSynonymAlter*)ast;
}

static inline AstSynonymAlter*
ast_synonym_alter_allocate(void)
{
	AstSynonymAlter* self;
	self = ast_allocate(0, sizeof(AstSynonymAlter));
	self->if_exists = false;
	str_init(&self->name);
	str_init(&self->name_new);
	return self;
}

void parse_synonym_create(Stmt*);
void parse_synonym_drop(Stmt*);
void parse_synonym_alter(Stmt*);
