#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstViewCreate AstViewCreate;
typedef struct AstViewDrop   AstViewDrop;
typedef struct AstViewAlter  AstViewAlter;

struct AstViewCreate
{
	Ast         ast;
	bool        if_not_exists;
	ViewConfig* config;
};

struct AstViewDrop
{
	Ast  ast;
	bool if_exists;
	Str  schema;
	Str  name;
};

struct AstViewAlter
{
	Ast         ast;
	bool        if_exists;
	Str         schema;
	Str         name;
	ViewConfig* config;
};

static inline AstViewCreate*
ast_view_create_of(Ast* ast)
{
	return (AstViewCreate*)ast;
}

static inline AstViewCreate*
ast_view_create_allocate(void)
{
	AstViewCreate* self;
	self = ast_allocate(STMT_CREATE_VIEW, sizeof(AstViewCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstViewDrop*
ast_view_drop_of(Ast* ast)
{
	return (AstViewDrop*)ast;
}

static inline AstViewDrop*
ast_view_drop_allocate(void)
{
	AstViewDrop* self;
	self = ast_allocate(STMT_DROP_VIEW, sizeof(AstViewDrop));
	self->if_exists = false;
	str_init(&self->schema);
	str_init(&self->name);
	return self;
}

static inline AstViewAlter*
ast_view_alter_of(Ast* ast)
{
	return (AstViewAlter*)ast;
}

static inline AstViewAlter*
ast_view_alter_allocate(void)
{
	AstViewAlter* self;
	self = ast_allocate(STMT_ALTER_SCHEMA, sizeof(AstViewAlter));
	self->if_exists = false;
	self->config    = NULL;
	str_init(&self->schema);
	str_init(&self->name);
	return self;
}

void parse_view_create(Stmt*);
void parse_view_drop(Stmt*);
void parse_view_alter(Stmt*);
