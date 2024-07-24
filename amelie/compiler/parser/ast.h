#pragma once

//
// amelie.
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
		Interval interval;
		Column*  column;
		struct {
			Str  string;
			bool string_escape;
		};
	};
	char* pos;
	int   priority;
	Ast*  l;
	Ast*  r;
	Ast*  next;
	Ast*  prev;
};

static inline void*
ast_allocate(int id, int size)
{
	assert(size >= (int)sizeof(Ast));
	Ast* self = palloc(size);
	memset(self, 0, size);
	self->pos = NULL;
	self->id = id;
	return self;
}

static inline Ast*
ast(int id)
{
	Ast* self = ast_allocate(id, sizeof(Ast));
	return self;
}
