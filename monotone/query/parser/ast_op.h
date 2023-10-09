#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstValue AstValue;
typedef struct AstRow   AstRow;

struct AstValue
{
	Ast  ast;
	Ast* expr;
};

struct AstRow
{
	Ast      ast;
	Ast*     list;
	int      list_count;
	int      data_size;
	uint32_t hash;
};

// value
static inline AstValue*
ast_value_of(Ast* ast)
{
	return (AstValue*)ast;
}

static inline AstValue*
ast_value_allocate(int id)
{
	AstValue* self = ast_allocate(id, sizeof(AstValue));
	self->expr = NULL;
	return self;
}

// row
static inline AstRow*
ast_row_of(Ast* ast)
{
	return (AstRow*)ast;
}

static inline AstRow*
ast_row_allocate(int id)
{
	AstRow* self = ast_allocate(id, sizeof(AstRow));
	self->list       = NULL;
	self->list_count = 0;
	self->data_size  = 0;
	self->hash       = 0;
	return self;
}
