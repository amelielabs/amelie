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
	IndexConfig* config_index;
};

struct AstTableDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

struct AstTableTruncate
{
	Ast ast;
	Str user;
	Str name;
};

enum
{
	TABLE_ALTER_RENAME,
	TABLE_ALTER_SET_IDENTITY,
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
	TABLE_ALTER_COLUMN_UNSET_RESOLVED,
	TABLE_ALTER_STORAGE_ADD,
	TABLE_ALTER_STORAGE_DROP,
	TABLE_ALTER_STORAGE_PAUSE,
	TABLE_ALTER_STORAGE_RESUME
};

struct AstTableAlter
{
	Ast     ast;
	bool    if_exists;
	bool    if_column_exists;
	bool    if_column_not_exists;
	bool    if_storage_exists;
	bool    if_storage_not_exists;
	int     type;
	Str     name;
	Str     name_new;
	Str     column_name;
	Column* column;
	Buf*    value_buf;
	Str     value;
	Ast*    identity;
	Volume* volume;
	Str     storage_name;
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
	return self;
}

void parse_key(Stmt*, Keys*);
void parse_table_create(Stmt*, bool);
void parse_table_drop(Stmt*);
void parse_table_alter(Stmt*);
void parse_table_truncate(Stmt*);
