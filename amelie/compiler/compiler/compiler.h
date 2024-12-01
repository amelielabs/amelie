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
	Code     code_coordinator;
	Code     code_node;
	CodeData code_data;
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
void compiler_parse_sql(Compiler*, Str*);
void compiler_parse_csv(Compiler*, Str*, Str*);
void compiler_emit(Compiler*);
void compiler_program(Compiler*, Program*);

static inline Stmt*
compiler_stmt(Compiler* self)
{
	return self->parser.stmt;
}

static inline TargetList*
compiler_target_list(Compiler* self)
{
	return &self->current->target_list;
}

static inline void
compiler_switch_coordinator(Compiler* self)
{
	self->code = &self->code_coordinator;
}

static inline void
compiler_switch_node(Compiler* self)
{
	self->code = &self->code_node;
}
