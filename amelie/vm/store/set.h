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

typedef struct Set Set;

struct Set
{
	Store   store;
	Buf     set;
	Buf     set_index;
	SetHash hash;
	int     count;
	int     count_rows;
	int     count_columns_row;
	int     count_columns;
	int     count_keys;
	bool    ordered;
	bool*   order;
};

always_inline static inline Value*
set_value(Set* self, int pos)
{
	return &((Value*)(self->set.start))[pos];
}

always_inline static inline Value*
set_row(Set* self, int pos)
{
	return set_value(self, self->count_columns_row * pos);
}

always_inline static inline Value*
set_row_ordered(Set* self, int pos)
{
	return set_row(self, ((uint32_t*)self->set_index.start)[pos]);
}

always_inline static inline Value*
set_row_of(Set* self, int pos)
{
	if (self->ordered)
		return set_row_ordered(self, pos);
	return set_row(self, pos);
}

always_inline static inline Value*
set_column(Set* self, int pos, int column)
{
	return set_row(self, pos) + column;
}

always_inline static inline Value*
set_column_of(Set* self, int pos, int column)
{
	return set_row_of(self, pos) + column;
}

always_inline static inline Value*
set_key(Set* self, int pos, int key)
{
	return set_row(self, pos) + self->count_columns + key;
}

Set* set_create(int, int, uint8_t*, bool);
void set_sort(Set*);
void set_add(Set*, Value*);
void set_add_from_stack(Set*, Stack*);
Value*
set_get_or_add(Set*, Value*);
