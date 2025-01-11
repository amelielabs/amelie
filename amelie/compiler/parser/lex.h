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

typedef struct Lex Lex;

struct Lex
{
	char*     pos;
	char*     end;
	char*     start;
	Keyword** keywords;
	bool      keywords_enable;
	Ast*      backlog;
};

void lex_init(Lex*, Keyword**);
void lex_reset(Lex*);
void lex_start(Lex*, Str*);
void lex_set_keywords(Lex*, bool);
void lex_error(Lex*, Ast*, const char*);
void lex_error_expect(Lex*, Ast*, int);
Ast* lex_next(Lex*);

static inline void
lex_push(Lex* self, Ast* ast)
{
	ast->next = NULL;
	ast->prev = self->backlog;
	self->backlog = ast;
}

hot static inline Ast*
lex_if(Lex* self, int id)
{
	auto ast = lex_next(self);
	if (ast->id == id)
		return ast;
	lex_push(self, ast);
	return NULL;
}
