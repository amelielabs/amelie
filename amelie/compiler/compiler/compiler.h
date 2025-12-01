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

typedef struct Compiler Compiler;

struct Compiler
{
	Stmt*        current;
	Code*        code;
	CodeData*    code_data;
	TargetOrigin origin;
	Rmap         map;
	Program*     program;
	Value*       program_args;
	Columns*     program_returning;
	Udf*         program_udf;
	bool         program_explain;
	bool         program_profile;
	SetCache*    set_cache;
	Parser       parser;
};

void compiler_init(Compiler*, Local*, SetCache*);
void compiler_free(Compiler*);
void compiler_reset(Compiler*);
void compiler_set(Compiler*, Program*);
void compiler_parse(Compiler*, Str*);
void compiler_parse_udf(Compiler*, Udf*);
void compiler_parse_endpoint(Compiler*, Endpoint*, Str*);
void compiler_emit(Compiler*);

static inline void
compiler_switch_frontend(Compiler* self)
{
	self->origin = ORIGIN_FRONTEND;
	self->code   = &self->program->code;
}

static inline void
compiler_switch_backend(Compiler* self)
{
	self->origin = ORIGIN_BACKEND;
	self->code = &self->program->code_backend;
}

static inline Namespace*
compiler_namespace(Compiler* self)
{
	return self->parser.nss.list;
}

static inline Block*
compiler_main(Compiler* self)
{
	return compiler_namespace(self)->blocks.list;
}

static inline Stmt*
compiler_stmt(Compiler* self)
{
	if (! compiler_namespace(self))
		return NULL;
	return compiler_main(self)->stmts.list;
}
