#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct AstValue  AstValue;
typedef struct AstRow    AstRow;
typedef struct AstInsert AstInsert;

struct AstValue
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

enum
{
	ON_CONFLICT_NONE,
	ON_CONFLICT_UPDATE_NONE,
	ON_CONFLICT_UPDATE
};

struct AstInsert
{
	Ast     ast;
	bool    unique;
	Target* target;
	Ast*    rows;
	int     rows_count;
	int     on_conflict;
	Ast*    update_expr;
	Ast*    update_where;
};

static inline AstValue*
ast_value_of(Ast* ast)
{
	return (AstValue*)ast;
}

static inline AstValue*
ast_value_allocate(void)
{
	AstValue* self = ast_allocate(0, sizeof(AstValue));
	self->expr = NULL;
	return self;
}

static inline AstRow*
ast_row_of(Ast* ast)
{
	return (AstRow*)ast;
}

static inline AstRow*
ast_row_allocate(void)
{
	AstRow* self = ast_allocate(0, sizeof(AstRow));
	self->list       = NULL;
	self->list_count = 0;
	return self;
}

static inline AstInsert*
ast_insert_of(Ast* ast)
{
	return (AstInsert*)ast;
}

static inline AstInsert*
ast_insert_allocate(void)
{
	AstInsert* self;
	self = ast_allocate(0, sizeof(AstInsert));
	self->unique       = false;
	self->target       = NULL;
	self->rows         = NULL;
	self->rows_count   = 0;
	self->on_conflict  = ON_CONFLICT_NONE;
	self->update_expr  = NULL;
	self->update_where = NULL;
	return self;
}

void parse_insert(Stmt*, bool);
