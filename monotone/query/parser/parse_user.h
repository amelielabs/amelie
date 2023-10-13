#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstUserCreate AstUserCreate;
typedef struct AstUserDrop   AstUserDrop;

struct AstUserCreate
{
	Ast         ast;
	bool        if_not_exists;
	UserConfig* config;
};

struct AstUserDrop
{
	Ast  ast;
	bool if_exists;
	Ast* name;
};

static inline AstUserCreate*
ast_user_create_of(Ast* ast)
{
	return (AstUserCreate*)ast;
}

static inline AstUserCreate*
ast_user_create_allocate(void)
{
	AstUserCreate* self;
	self = ast_allocate(STMT_CREATE_USER, sizeof(AstUserCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstUserDrop*
ast_user_drop_of(Ast* ast)
{
	return (AstUserDrop*)ast;
}

static inline AstUserDrop*
ast_user_drop_allocate(void)
{
	AstUserDrop* self;
	self = ast_allocate(STMT_DROP_USER, sizeof(AstUserDrop));
	self->if_exists = false;
	self->name      = NULL;
	return self;
}

void parse_user_create(Stmt*);
void parse_user_drop(Stmt*);
void parse_user_alter(Stmt*);
