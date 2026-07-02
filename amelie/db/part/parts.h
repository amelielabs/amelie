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

typedef struct PartsIf PartsIf;
typedef struct Parts   Parts;

struct PartsIf
{
	void (*attach)(Parts*);
	void (*detach)(Parts*);
};

struct Parts
{
	PartMapping mapping;
	List        list;
	int         list_count;
	PartArg*    arg;
	PartsIf*    iface;
	void*       iface_arg;
};

void  parts_init(Parts*, PartsIf*, void*, PartArg*, Keys*);
void  parts_free(Parts*);
void  parts_open(Parts*, List*, List*);
void  parts_close(Parts*);
void  parts_truncate(Parts*);
void  parts_add(Parts*, Part*);
void  parts_remove(Parts*, Part*);
Part* parts_find(Parts*, uint64_t);
void  parts_index_create(Parts*, IndexConfig*);
void  parts_index_remove(Parts*, Str*);
void  parts_column_create(Parts*, Column*);
void  parts_list(Parts*, Buf*, Str*, int);
