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

typedef struct Merge Merge;

struct Merge
{
	Store   store;
	Buf     list;
	int     list_count;
	bool    distinct;
	int64_t limit;
	int64_t offset;
};

Merge* merge_create(bool, int64_t, int64_t);
void   merge_add(Merge*, Set*);
