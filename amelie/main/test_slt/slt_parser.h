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

typedef struct SltParser SltParser;

struct SltParser
{
	Str    stream;
	int    stream_line;
	Buf*   data;
	SltCmd cmd;
};

void    slt_parser_init(SltParser*);
void    slt_parser_free(SltParser*);
void    slt_parser_open(SltParser*, Str*);
SltCmd* slt_parser_next(SltParser*);
