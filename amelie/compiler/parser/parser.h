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

typedef struct Parser Parser;

struct Parser
{
	Namespaces nss;
	bool       explain;
	bool       profile;
	Program*   program;
	SetCache*  set_cache;
	Uri        uri;
	Json       json;
	Lex        lex;
	Str*       db;
	Local*     local;
};

void parser_init(Parser*, Local*, Str*, SetCache*);
void parser_reset(Parser*);
void parser_free(Parser*);
