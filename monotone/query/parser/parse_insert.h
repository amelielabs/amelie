#pragma once

//
// monotone
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

struct AstInsert
{
	Ast    ast;
	bool   unique;
	Table* table;
	Ast*   rows;
	int    rows_count;
	bool   generate;
	Ast*   generate_count;
	Ast*   generate_batch;
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
	self->unique         = false;
	self->table          = NULL;
	self->rows           = NULL;
	self->rows_count     = 0;
	self->generate       = false;
	self->generate_count = NULL;
	self->generate_batch = NULL;
	return self;
}

void parse_insert(Stmt*, bool);
