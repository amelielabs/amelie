#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
	bool asc;
};

struct Set
{
	Store   store;
	Buf     list;
	int     list_count;
	SetKey* keys;
	int     keys_count;
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

hot static inline int
set_compare(SetKey* keys, int keys_count, SetRow* a, SetRow* b)
{
	for (int i = 0; i < keys_count; i++)
	{
		auto key = &keys[i];
		int rc = value_compare(&a->keys[i], &b->keys[i]);
		if (rc != 0)
			return (key->asc) ? rc : -rc;
	}
	return 0;
}
