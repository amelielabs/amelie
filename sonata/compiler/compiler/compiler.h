#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Compiler Compiler;

struct Compiler
{
	Parser       parser;
	Rmap         map;
	Code*        code;
	Code         code_coordinator;
	Code         code_shard;
	CodeData     code_data;
	bool         snapshot;
	Stmt*        current;
	Stmt*        last;
	FunctionMgr* function_mgr;
	Db*          db;
};

void compiler_init(Compiler*, Db*, FunctionMgr*);
void compiler_free(Compiler*);
void compiler_reset(Compiler*);
void compiler_parse(Compiler*, Str*);
void compiler_emit(Compiler*);

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
compiler_switch_shard(Compiler* self)
{
	self->code = &self->code_shard;
}
