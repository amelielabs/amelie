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
	SetKey* keys;
	int     keys_count;
	Buf     list;
	int     list_count;
	int64_t limit;
	int64_t offset;
	bool    distinct;
};

Merge* merge_create(void);
void   merge_add(Merge*, Set*);
void   merge_open(Merge*, bool, int64_t, int64_t);
