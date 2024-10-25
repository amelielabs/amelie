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
	char*       pos;
	char*       end;
	Keyword**   keywords;
	bool        keywords_enable;
	Ast*        backlog;
	const char* prefix;
};

void lex_init(Lex*, Keyword**);
void lex_reset(Lex*);
void lex_start(Lex*, Str*);
void lex_set_keywords(Lex*, bool);
void lex_push(Lex*, Ast*);
Ast* lex_next(Lex*);
Ast* lex_if(Lex*, int);
