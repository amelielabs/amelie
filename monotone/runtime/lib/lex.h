#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Token   Token;
typedef struct Keyword Keyword;
typedef struct Lex     Lex;

enum
{
	LEX_EOF = 128,
	LEX_FLOAT,
	LEX_INT,
	LEX_ARGUMENT,
	LEX_ARGUMENT_NAME,
	LEX_STRING,
	LEX_SHL,
	LEX_SHR,
	LEX_LTE,
	LEX_GTE,
	LEX_NEQU,
	LEX_CAT,
	LEX_NAME,
	LEX_NAME_COMPOUND,
	LEX_NAME_COMPOUND_ASTERISK,
	LEX_KEYWORD
};

struct Token
{
	int type;
	int line;
	union {
		uint64_t integer;
		float    fp;
		struct {
			Str  string;
			bool escape;
		};
	};
};

struct Keyword
{
	int         id;
	const char* name;
	int         name_size;
};

struct Lex
{
	char*       pos;
	char*       end;
	int         line;
	Token       backlog[4];
	int         backlog_count;
	bool        keywords_enabled;
	Keyword**   keywords;
	const char* prefix;
};

void lex_init_token(Token*);
void lex_init(Lex*, Keyword**, Str*);
void lex_enable_keywords(Lex*, bool);
void lex_enable_prefix(Lex*, const char*);
void lex_push(Lex*, Token*);
int  lex_next(Lex*, Token*);
bool lex_if(Lex*, Token*, int);
