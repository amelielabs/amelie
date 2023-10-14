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
	Buf          data;
	Stmt*        current;
	Code         code_shared;
	Code*        code;
	CompilerCode code_get;
	void*        code_get_arg;
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
	parser_init(&self->parser, db);
	rmap_init(&self->map);
	buf_init(&self->data);
}

static inline void
compiler_free(Compiler* self)
{
	buf_free(&self->data);
	rmap_free(&self->map);
}

static inline void
compiler_reset(Compiler* self)
{
	self->current      = NULL;
	self->code         = NULL;
	self->code_get     = NULL;
	self->code_get_arg = NULL;
	buf_reset(&self->data);
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
