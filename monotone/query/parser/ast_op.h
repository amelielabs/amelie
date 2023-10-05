#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstCall AstCall;
typedef struct AstExpr AstExpr;
typedef struct AstRow  AstRow;

struct AstCall
{
	Ast  ast;
	Ast* name;
	int  count;
};

struct AstExpr
{
	Ast  ast;
	Ast* expr;
};

struct AstRow
{
	Ast  ast;
	Ast* list;
	int  list_count;
};

// call
static inline AstCall*
ast_call_of(Ast* ast)
{
	return (AstCall*)ast;
}

static inline Ast*
ast_call_allocate(int id, Ast* name, int count)
{
	AstCall* self = ast_allocate(id, sizeof(AstCall));
	self->name  = name;
	self->count = count;
	return &self->ast;
}

// expr
static inline AstExpr*
ast_expr_of(Ast* ast)
{
	return (AstExpr*)ast;
}

static inline Ast*
ast_expr_allocate(int id, Ast* expr)
{
	AstExpr* self = ast_allocate(id, sizeof(AstExpr));
	self->expr = expr;
	return &self->ast;
}

// row
static inline AstRow*
ast_row_of(Ast* ast)
{
	return (AstRow*)ast;
}

static inline Ast*
ast_row_allocate(int id, Ast* list, int list_count)
{
	AstRow* self = ast_allocate(id, sizeof(AstRow));
	self->list = list;
	self->list_count = list_count;
	return &self->ast;
}
