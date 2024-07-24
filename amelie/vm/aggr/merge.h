#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Merge Merge;

struct Merge
{
	ValueObj obj;
	SetKey*  keys;
	int      keys_count;
	Buf      list;
	int      list_count;
	int64_t  limit;
	int64_t  offset;
	bool     distinct;
};

Merge* merge_create(void);
void   merge_add(Merge*, Set*);
void   merge_open(Merge*, bool, int64_t, int64_t);
