#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstShow AstShow;
typedef struct AstSet  AstSet;

struct AstShow
{
	Ast  ast;
	Ast* expr;
};

struct AstSet
{
	Ast  ast;
	Ast* name;
	Ast* value;
};

static inline AstShow*
ast_show_of(Ast* ast)
{
	return (AstShow*)ast;
}

static inline AstShow*
ast_show_allocate(Ast* expr)
{
	AstShow* self = ast_allocate(STMT_SHOW, sizeof(AstShow));
	self->expr = expr;
	return self;
}

static inline AstSet*
ast_set_of(Ast* ast)
{
	return (AstSet*)ast;
}

static inline AstSet*
ast_set_allocate(Ast* name, Ast* value)
{
	AstSet* self = ast_allocate(STMT_SET, sizeof(AstSet));
	self->name  = name;
	self->value = value;
	return self;
}
