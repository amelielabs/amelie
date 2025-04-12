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
	Parser   parser;
	Rmap     map;
	Code*    code;
	Code     code_frontend;
	Code     code_backend;
	CodeData code_data;
	Access   access;
	SetCache values_cache;
	Columns* args;
	bool     snapshot;
	Stmt*    current;
	Stmt*    last;
	Db*      db;
};

void compiler_init(Compiler*, Db*, Local*, FunctionMgr*);
void compiler_free(Compiler*);
void compiler_reset(Compiler*);
void compiler_parse(Compiler*, Str*);
void compiler_parse_import(Compiler*, Str*, Str*, EndpointType);
void compiler_emit(Compiler*);
void compiler_program(Compiler*, Program*);

static inline Stmt*
compiler_stmt(Compiler* self)
{
	return self->parser.stmt;
}

static inline void
compiler_switch_frontend(Compiler* self)
{
	self->code = &self->code_frontend;
}

static inline void
compiler_switch_backend(Compiler* self)
{
	self->code = &self->code_backend;
}
