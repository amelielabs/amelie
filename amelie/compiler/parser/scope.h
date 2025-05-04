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

typedef struct Scope  Scope;
typedef struct Scopes Scopes;
typedef struct Parser Parser;

struct Scope
{
	Lex*     lex;
	Stmt*    stmt;
	Stmts    stmts;
	Vars     vars;
	Ctes     ctes;
	Columns* args;
	Parser*  parser;
	Scope*   next;
};

struct Scopes
{
	Scope* list;
	Scope* list_tail;
	int    count;
};

static inline Scope*
scope_allocate(Parser* parser, Columns* args)
{
	Scope* self = palloc(sizeof(Scope));
	self->lex    = NULL;
	self->stmt   = NULL;
	self->args   = args;
	self->next   = NULL;
	self->parser = parser;
	vars_init(&self->vars);
	ctes_init(&self->ctes);
	stmts_init(&self->stmts);
	return self;
}

always_inline hot static inline Ast*
scope_next_shadow(Scope* self)
{
	lex_set_keywords(self->lex, false);
	auto ast = lex_next(self->lex);
	lex_set_keywords(self->lex, true);
	return ast;
}

always_inline hot static inline Ast*
scope_next(Scope* self)
{
	return lex_next(self->lex);
}

always_inline hot static inline Ast*
scope_if(Scope* self, int id)
{
	return lex_if(self->lex, id);
}

static inline void
scope_error(Scope* self, Ast* ast, const char* fmt, ...)
{
	va_list args;
	char msg[256];
	va_start(args, fmt);
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);
	if (! ast)
		ast = lex_next(self->lex);
	lex_error(self->lex, ast, msg);
}

always_inline hot static inline Ast*
scope_expect(Scope* self, int id)
{
	auto ast = lex_next(self->lex);
	if (ast->id == id)
		return ast;
	lex_error_expect(self->lex, ast, id);
	return NULL;
}

always_inline static inline void
scope_push(Scope* self, Ast* ast)
{
	lex_push(self->lex, ast);
}

static inline void
scopes_init(Scopes* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline void
scopes_add(Scopes* self, Scope* scope)
{
	if (self->list == NULL)
		self->list = scope;
	else
		self->list_tail->next = scope;
	self->list_tail = scope;
	self->count++;
}
