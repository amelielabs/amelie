#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Compiler Compiler;

typedef Code* (*CompilerCode)(uint32_t, void*);

struct Compiler
{
	Parser       parser;
	Rmap         map;
	Stmt*        current;
	Code*        code;
	CompilerCode code_get;
	void*        code_get_arg;
	Code         code_stmt;
	CodeData     code_data;
	Db*          db;
};

static inline void
compiler_init(Compiler *self, Db* db)
{
	self->current      = NULL;
	self->code         = NULL;
	self->code_get     = NULL;
	self->code_get_arg = NULL;
	self->db           = db;
	code_init(&self->code_stmt);
	code_data_init(&self->code_data);
	parser_init(&self->parser, db);
	rmap_init(&self->map);
}

static inline void
compiler_free(Compiler* self)
{
	code_free(&self->code_stmt);
	code_data_free(&self->code_data);
	rmap_free(&self->map);
}

static inline void
compiler_reset(Compiler* self)
{
	self->current      = NULL;
	self->code         = NULL;
	self->code_get     = NULL;
	self->code_get_arg = NULL;
	code_reset(&self->code_stmt);
	code_data_reset(&self->code_data);
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

static inline void
compiler_set_code(Compiler* self, Code* code)
{
	self->code = code;
}

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

void compiler_parse(Compiler*, Str*);
void compiler_generate(Compiler*, CompilerCode, void*);
bool compiler_is_utility(Compiler*);
