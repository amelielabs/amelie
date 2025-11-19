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

typedef struct Union Union;

typedef enum
{
	UNION_ALL,
	UNION_ORDERED,
	UNION_ORDERED_DISTINCT,
	UNION_JOIN,
	UNION_EXCEPT
} UnionType;

struct Union
{
	Store     store;
	UnionType type;
	Buf*      list;
	int       list_count;
	int64_t   limit;
	int64_t   offset;
};

Union* union_create(UnionType);
void   union_add(Union*, Store*);

static inline void
union_set_limit(Union* self, int64_t value)
{
	self->limit = value;
}

static inline void
union_set_offset(Union* self, int64_t value)
{
	self->offset = value;
}
