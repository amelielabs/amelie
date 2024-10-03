#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AstTokenCreate AstTokenCreate;

struct AstTokenCreate
{
	Ast  ast;
	Ast* user;
	Ast* expire;
};

static inline AstTokenCreate*
ast_token_create_of(Ast* ast)
{
	return (AstTokenCreate*)ast;
}

static inline AstTokenCreate*
ast_token_create_allocate(void)
{
	AstTokenCreate* self;
	self = ast_allocate(0, sizeof(AstTokenCreate));
	self->user   = NULL;
	self->expire = NULL;
	return self;
}

void parse_token_create(Stmt*);
