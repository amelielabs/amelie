#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Ast Ast;

struct Ast
{
	int id;
	union {
		uint64_t integer;
		double   real;
		struct {
			Str  string;
			bool string_escape;
		};
		Column* column;
	};
	int  priority;
	Ast* l;
	Ast* r;
	Ast* next;
	Ast* prev;
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
