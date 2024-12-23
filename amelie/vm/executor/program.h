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

typedef struct Program Program;

struct Program
{
	Code*     code;
	Code*     code_node;
	CodeData* code_data;
	int       stmts;
	int       stmts_last;
	bool      snapshot;
	bool      repl;
};

static inline void
program_init(Program* self)
{
	memset(self, 0, sizeof(*self));
	self->stmts_last = -1;
}
