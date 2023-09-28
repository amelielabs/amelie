#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SetRow SetRow;
typedef struct SetKey SetKey;
typedef struct Set    Set;

struct SetRow
{
	Value value;
	Value keys[];
};

struct SetKey
{
	int64_t type;
	bool    asc;
};

struct Set
{
	ValueObj obj;
	Buf      list;
	int      list_count;
	SetKey*  keys;
	int      keys_count;
};

static inline SetRow*
set_at(Set* self, int pos)
{
	return ((SetRow**)(self->list.start))[pos];
}

Set* set_create(uint8_t*);
void set_add(Set*, Value*, Value**);
void set_add_from_stack(Set*, Value*, Stack*);
void set_sort(Set*);
