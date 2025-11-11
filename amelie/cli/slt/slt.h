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

typedef struct Slt Slt;

struct Slt
{
	Str*              dir;
	int               line;
	int               threshold;
	Json              json;
	amelie_session_t* session;
	amelie_t*         env;
};

void slt_init(Slt*);
void slt_start(Slt*, Str*, Str*);
void slt_stop(Slt*);
