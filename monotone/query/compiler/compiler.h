#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct CompilerIf CompilerIf;
typedef struct Compiler   Compiler;

struct CompilerIf
{
	Code* (*match)(uint32_t, void*);
	void  (*apply)(uint32_t, Code*, void*);
	void  (*end)(void*);
};

struct Compiler
{
	Parser      parser;
	Rmap        map;
	Stmt*       current;
	Code*       code;
	Code        code_stmt;
	CodeData    code_data;
	CompilerIf* iface;
	void*       iface_arg;
	Db*         db;
};

static inline void
compiler_init(Compiler *self, Db* db, CompilerIf* iface, void* iface_arg)
{
	self->current   = NULL;
	self->code      = NULL;
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->db        = db;
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
	self->current = NULL;
	self->code    = NULL;
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
void compiler_emit(Compiler*);
bool compiler_is_utility(Compiler*);
