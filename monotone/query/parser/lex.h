#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Lex Lex;

struct Lex
{
	char*       pos;
	char*       end;
	Keyword**   keywords;
	Ast*        backlog;
	const char* prefix;
};

void lex_init(Lex*, Keyword**);
void lex_reset(Lex*);
void lex_start(Lex*, Str*);
void lex_push(Lex*, Ast*);
Ast* lex_next(Lex*);
Ast* lex_if(Lex*, int);
