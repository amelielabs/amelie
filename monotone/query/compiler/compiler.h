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
	Code*        code;
	CompilerCode code_get;
	void*        code_get_arg;
	Db*          db;
};

static inline void
compiler_init(Compiler *self, Db* db)
{
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
	parser_free(&self->parser);
}

static inline void
compiler_reset(Compiler* self)
{
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

static inline Ast*
compiler_first(Compiler* self)
{
	return self->parser.query.stmts.list->ast;
}

void compiler_parse(Compiler*, Str*);
void compiler_generate(Compiler*, CompilerCode, void*);
bool compiler_is_utility(Compiler*);
