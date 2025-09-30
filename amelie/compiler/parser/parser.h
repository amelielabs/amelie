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
	Blocks    blocks;
	Program*  program;
	SetCache* values_cache;
	Uri       uri;
	Json      json;
	Lex       lex;
	Local*    local;
};

void parser_init(Parser*, Local*, SetCache*);
void parser_reset(Parser*);
void parser_free(Parser*);
