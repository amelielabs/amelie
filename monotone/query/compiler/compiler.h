#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Compiler Compiler;

struct Compiler
{
	Parser    parser;
	Rmap      map;
	Stmt*     current;
	Code      code_coordinator;
	Code      code;
	CodeData  code_data;
	Dispatch* dispatch;
	Router*   router;
	Db*       db;
};

void compiler_init(Compiler*, Db*, Router*, Dispatch*);
void compiler_free(Compiler*);
void compiler_reset(Compiler*);
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
