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

typedef struct Opts    Opts;
typedef struct OptsDef OptsDef;

struct Opts
{
	List list;
	int  list_count;
};

struct OptsDef
{
	const char* name;
	OptType     type;
	int         flags;
	Opt*        opt;
	char*       default_string;
	uint64_t    default_int;
};

void opts_init(Opts*);
void opts_free(Opts*);
void opts_define(Opts*, OptsDef*);
bool opts_set_json(Opts*, uint8_t**);
bool opts_set(Opts*, Str*);
bool opts_set_argv(Opts*, int, char**);
Buf* opts_list(Opts*);
Buf* opts_list_persistent(Opts*);
Opt* opts_find(Opts*, Str*);
void opts_print(Opts*);
