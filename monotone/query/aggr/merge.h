#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Merge Merge;

struct Merge
{
	ValueObj     obj;
	SetRow*      current;
	SetIterator* current_it;
	SetKey*      keys;
	int          keys_count;
	Buf          list;
	int          list_count;
	int64_t      limit;
	bool         distinct;
};

Merge* merge_create(void);
void   merge_add(Merge*, Set*);
void   merge_next(Merge*);
void   merge_open(Merge*, bool, int64_t, int64_t);

static inline bool
merge_has(Merge* self)
{
	return self->current != NULL;
}

static inline SetRow*
merge_at(Merge* self)
{
	return self->current;
}
