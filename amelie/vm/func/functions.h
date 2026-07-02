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

typedef struct Functions Functions;

struct Functions
{
	Hashtable ht;
	List      list;
	int       list_count;
};

void functions_init(Functions*);
void functions_free(Functions*);
void functions_add(Functions*, Function*);
void functions_del(Functions*, Function*);
Function*
functions_find(Functions*, Str*);
