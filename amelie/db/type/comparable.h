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

typedef struct ComparableKey ComparableKey;
typedef struct Comparable    Comparable;

enum
{
	COMPARE_I64,
	COMPARE_I32,
	COMPARE_UUID,
	COMPARE_STR
};

struct ComparableKey
{
	uint16_t type;
	uint16_t column;
};

struct Comparable
{
	Buf keys;
	int keys_count;
};

static inline void
comparable_init(Comparable* self)
{
	self->keys_count = 0;
	buf_init(&self->keys);
}

static inline void
comparable_free(Comparable* self)
{
	buf_free(&self->keys);
}

static inline void
comparable_add(Comparable* self, Column* column)
{
	int type;
	if (column->size == 8)
		type = COMPARE_I64;
	else
	if (column->size == 4)
		type = COMPARE_I32;
	else
	if (column->size == sizeof(Uuid))
		type = COMPARE_UUID;
	else
		type = COMPARE_STR;
	ComparableKey key =
	{
		.type   = type,
		.column = column->order
	};
	buf_write(&self->keys, &key, sizeof(key));
	self->keys_count++;
}
