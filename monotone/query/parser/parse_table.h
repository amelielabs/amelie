#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstTableCreate AstTableCreate;
typedef struct AstTableDrop   AstTableDrop;

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
	Ast* name;
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
	self = ast_allocate(STMT_CREATE_TABLE, sizeof(AstTableCreate));
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
	self = ast_allocate(STMT_DROP_TABLE, sizeof(AstTableDrop));
	self->if_exists = false;
	self->name      = NULL;
	return self;
}

void parse_table_create(Stmt*);
void parse_table_drop(Stmt*);
void parse_table_alter(Stmt*);
