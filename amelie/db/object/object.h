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

typedef struct Object Object;

struct Object
{
	Id         id;
	Branch*    branches;
	int        branches_count;
	Branch*    root;
	File       file;
	RbtreeNode link_mapping;
	List       link_volume;
	List       link;
};

Object*
object_allocate(Id*);
void object_free(Object*);
void object_open(Object*, int);
void object_create(Object*, int);
void object_delete(Object*, int);
void object_rename(Object*, int, int);
void object_add(Object*, Branch*);
void object_status(Object*, Buf*, int, Str*);
