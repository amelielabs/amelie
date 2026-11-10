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

typedef struct AstDatabaseCreate AstDatabaseCreate;
typedef struct AstDatabaseDrop   AstDatabaseDrop;
typedef struct AstDatabaseAlter  AstDatabaseAlter;

struct AstDatabaseCreate
{
	Ast       ast;
	bool      if_not_exists;
	DbConfig* config;
};

struct AstDatabaseDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

enum
{
	DATABASE_ALTER_RENAME,
	DATABASE_ALTER_DESCRIPTION,
	DATABASE_ALTER_REVOKE_TOKEN,
	DATABASE_ALTER_LIMIT_SET,
	DATABASE_ALTER_LIMIT_UNSET
};

struct AstDatabaseAlter
{
	Ast      ast;
	bool     if_exists;
	int      type;
	Str      name;
	Str      name_new;
	Str      revoked_at;
	Str      description;
	Limits   limits;
	uint64_t limits_mask;
};

static inline AstDatabaseCreate*
ast_database_create_of(Ast* ast)
{
	return (AstDatabaseCreate*)ast;
}

static inline AstDatabaseCreate*
ast_database_create_allocate(void)
{
	AstDatabaseCreate* self;
	self = ast_allocate(0, sizeof(AstDatabaseCreate));
	return self;
}

static inline AstDatabaseDrop*
ast_database_drop_of(Ast* ast)
{
	return (AstDatabaseDrop*)ast;
}

static inline AstDatabaseDrop*
ast_database_drop_allocate(void)
{
	AstDatabaseDrop* self;
	self = ast_allocate(0, sizeof(AstDatabaseDrop));
	return self;
}

static inline AstDatabaseAlter*
ast_database_alter_of(Ast* ast)
{
	return (AstDatabaseAlter*)ast;
}

static inline AstDatabaseAlter*
ast_database_alter_allocate(void)
{
	AstDatabaseAlter* self;
	self = ast_allocate(0, sizeof(AstDatabaseAlter));
	return self;
}

void parse_database_create(Stmt*);
void parse_database_drop(Stmt*);
void parse_database_alter(Stmt*);
