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

typedef struct Json Json;

struct Json
{
	char* pos;
	char* end;
	Buf   stack;
	Buf*  buf;
	Buf   buf_data;
};

void json_init(Json*);
void json_free(Json*);
void json_reset(Json*);
void json_parse(Json*, Str*, Buf*);
