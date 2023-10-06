#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Ast Ast;

typedef enum
{
	AST_UNDEF,
	AST_SIMPLE,    // pre-computable or const
	AST_COMPLEX,   // name + simple_expr
	AST_NAME,      // name
	AST_OP,        // name > simple_expr
	AST_CONDITION  // AND, OR, NOT (1 and 1 == simple)
} AstType;

struct Ast
{
	int id;
	union {
		uint64_t integer;
		float    fp;
		struct {
			Str  string;
			bool string_escape;
		};
	};
	AstType type;
	Ast*    next;
	Ast*    prev;
};

static inline void*
ast_allocate(int id, int size)
{
	assert(size >= (int)sizeof(Ast));
	Ast* self = palloc(size);
	self->id   = id;
	self->next = NULL;
	self->prev = NULL;
	return self;
}

static inline Ast*
ast(int id)
{
	Ast* self = ast_allocate(id, sizeof(Ast));
	return self;
}
