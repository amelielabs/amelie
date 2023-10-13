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
	int     priority;
	Ast*    l;
	Ast*    r;
	Ast*    next;
	Ast*    prev;
};

static inline void*
ast_allocate(int id, int size)
{
	assert(size >= (int)sizeof(Ast));
	Ast* self = palloc(size);
	memset(self, 0, size);
	self->id = id;
	return self;
}

static inline Ast*
ast(int id)
{
	Ast* self = ast_allocate(id, sizeof(Ast));
	return self;
}
