#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct Ast Ast;

struct Ast
{
	int     id;
	uint8_t priority;
	union {
		int64_t integer;
		double  real;
		Column* column;
		struct {
			Str  string;
			bool string_escape;
		};
	};
	int  pos_start;
	int  pos_end;
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
