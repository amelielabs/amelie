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

typedef struct AstShow AstShow;
typedef struct AstSet  AstSet;

enum
{
	SHOW_USERS,
	SHOW_USER,
	SHOW_REPLICAS,
	SHOW_REPLICA,
	SHOW_REPL,
	SHOW_WAL,
	SHOW_METRICS,
	SHOW_SCHEMAS,
	SHOW_SCHEMA,
	SHOW_TABLES,
	SHOW_TABLE,
	SHOW_PROCEDURES,
	SHOW_PROCEDURE,
	SHOW_STATE,
	SHOW_CONFIG_ALL,
	SHOW_CONFIG
};

struct AstShow
{
	Ast  ast;
	Str  section;
	Ast* name_ast;
	Str  name;
	Str  schema;
	Str  format;
	int  type;
	bool extended;
};

static inline AstShow*
ast_show_of(Ast* ast)
{
	return (AstShow*)ast;
}

static inline AstShow*
ast_show_allocate(void)
{
	AstShow* self = ast_allocate(0, sizeof(AstShow));
	return self;
}

void parse_show(Stmt*);
