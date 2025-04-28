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
	Code*     code_backend;
	CodeData* code_data;
	Access*   access;
	int       sends;
	bool      snapshot;
	bool      repl;
};

static inline void
program_init(Program* self)
{
	memset(self, 0, sizeof(*self));
}
