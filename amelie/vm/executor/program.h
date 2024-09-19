#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Program Program;

struct Program
{
	Code*     code;
	Code*     code_node;
	CodeData* code_data;
	int       stmts;
	int       stmts_last;
	int       ctes;
	bool      snapshot;
	bool      repl;
};

static inline void
program_init(Program* self)
{
	memset(self, 0, sizeof(*self));
	self->stmts_last = -1;
}
