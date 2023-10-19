#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Compiler Compiler;

struct Compiler
{
	Parser       parser;
	Rmap         map;
	Stmt*        current;
	Code*        code;
	Code         code_coordinator;
	Code         code_stmt;
	CodeData     code_data;
	Dispatch*    dispatch;
	Router*      router;
	FunctionMgr* function_mgr;
	Db*          db;
};

void compiler_init(Compiler*, Db*, FunctionMgr*, Router*, Dispatch*);
void compiler_free(Compiler*);
void compiler_reset(Compiler*);
void compiler_switch(Compiler*, bool);
void compiler_parse(Compiler*, Str*);
void compiler_emit(Compiler*);
bool compiler_is_utility(Compiler*);

static inline Stmt*
compiler_first(Compiler* self)
{
	return container_of(self->parser.stmts.next, Stmt, link);
}

static inline TargetList*
compiler_target_list(Compiler* self)
{
	return &self->current->target_list;
}
