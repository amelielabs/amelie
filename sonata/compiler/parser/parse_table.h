#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstTableCreate AstTableCreate;
typedef struct AstTableDrop   AstTableDrop;
typedef struct AstTableAlter  AstTableAlter;

struct AstTableCreate
{
	Ast          ast;
	bool         if_not_exists;
	TableConfig* config;
};

struct AstTableDrop
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
};

struct AstTableAlter
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  schema_new;
	Str  name;
	Str  name_new;
	Ast* serial;
};

static inline AstTableCreate*
ast_table_create_of(Ast* ast)
{
	return (AstTableCreate*)ast;
}

static inline AstTableCreate*
ast_table_create_allocate(void)
{
	AstTableCreate* self;
	self = ast_allocate(0, sizeof(AstTableCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstTableDrop*
ast_table_drop_of(Ast* ast)
{
	return (AstTableDrop*)ast;
}

static inline AstTableDrop*
ast_table_drop_allocate(void)
{
	AstTableDrop* self;
	self = ast_allocate(0, sizeof(AstTableDrop));
	self->if_exists = false;
	str_init(&self->schema);
	str_init(&self->name);
	return self;
}

static inline AstTableAlter*
ast_table_alter_of(Ast* ast)
{
	return (AstTableAlter*)ast;
}

static inline AstTableAlter*
ast_table_alter_allocate(void)
{
	AstTableAlter* self;
	self = ast_allocate(0, sizeof(AstTableAlter));
	self->if_exists = false;
	self->serial    = NULL;
	str_init(&self->schema);
	str_init(&self->schema_new);
	str_init(&self->name);
	str_init(&self->name_new);
	return self;
}

int  parse_type(Stmt* self, Column*, Str*);
void parse_key(Stmt*, Keys*);
void parse_table_create(Stmt*);
void parse_table_drop(Stmt*);
void parse_table_alter(Stmt*);
