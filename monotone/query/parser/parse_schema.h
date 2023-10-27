#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstSchemaCreate AstSchemaCreate;
typedef struct AstSchemaDrop   AstSchemaDrop;

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
	Ast* name;
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
	self = ast_allocate(STMT_CREATE_SCHEMA, sizeof(AstSchemaCreate));
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
	self = ast_allocate(STMT_DROP_SCHEMA, sizeof(AstSchemaDrop));
	self->if_exists = false;
	self->name      = NULL;
	return self;
}

void parse_schema_create(Stmt*);
void parse_schema_drop(Stmt*);
void parse_schema_alter(Stmt*);
