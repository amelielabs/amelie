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

typedef struct AstTableCreate   AstTableCreate;
typedef struct AstTableDrop     AstTableDrop;
typedef struct AstTableTruncate AstTableTruncate;
typedef struct AstTableAlter    AstTableAlter;

struct AstTableCreate
{
	Ast          ast;
	bool         if_not_exists;
	TableConfig* config;
	int          partitions;
};

struct AstTableDrop
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
};

struct AstTableTruncate
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
};

enum
{
	TABLE_ALTER_RENAME,
	TABLE_ALTER_SET_IDENTITY,
	TABLE_ALTER_SET_UNLOGGED,
	TABLE_ALTER_COLUMN_ADD,
	TABLE_ALTER_COLUMN_DROP,
	TABLE_ALTER_COLUMN_RENAME,
	TABLE_ALTER_COLUMN_SET_DEFAULT,
	TABLE_ALTER_COLUMN_SET_IDENTITY,
	TABLE_ALTER_COLUMN_SET_STORED,
	TABLE_ALTER_COLUMN_SET_RESOLVED,
	TABLE_ALTER_COLUMN_UNSET_DEFAULT,
	TABLE_ALTER_COLUMN_UNSET_IDENTITY,
	TABLE_ALTER_COLUMN_UNSET_STORED,
	TABLE_ALTER_COLUMN_UNSET_RESOLVED
};

struct AstTableAlter
{
	Ast     ast;
	bool    if_exists;
	int     type;
	Str     schema;
	Str     schema_new;
	Str     name;
	Str     name_new;
	Str     column_name;
	Column* column;
	Buf*    value_buf;
	Str     value;
	bool    unlogged;
	Ast*    identity;
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
	self->partitions    = 0;
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

static inline AstTableTruncate*
ast_table_truncate_of(Ast* ast)
{
	return (AstTableTruncate*)ast;
}

static inline AstTableTruncate*
ast_table_truncate_allocate(void)
{
	AstTableTruncate* self;
	self = ast_allocate(0, sizeof(AstTableTruncate));
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
	self->if_exists  = false;
	self->type       = 0;
	self->column     = NULL;
	self->value_buf  = NULL;
	self->identity   = NULL;
	self->unlogged   = false;
	str_init(&self->schema);
	str_init(&self->schema_new);
	str_init(&self->name);
	str_init(&self->name_new);
	str_init(&self->column_name);
	str_init(&self->value);
	return self;
}

bool parse_type(Stmt*, int*, int*);
void parse_key(Stmt*, Keys*);
void parse_table_create(Stmt*, bool);
void parse_table_drop(Stmt*);
void parse_table_alter(Stmt*);
void parse_table_truncate(Stmt*);
