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

typedef struct Key Key;

struct Key
{
	int     order;
	Column* column;
	int64_t column_order;
	bool    asc;
};

static inline void
key_init(Key* self)
{
	self->order        = -1;
	self->column       = NULL;
	self->column_order = -1;
	self->asc          = true;
}

static inline void
key_set_column_order(Key* self, int value)
{
	self->column_order = value;
}

static inline void
key_set_asc(Key* self, bool value)
{
	self->asc = value;
}

static inline void
key_read(Key* self, uint8_t** pos)
{
	Decode obj[] =
	{
		{ DECODE_INT,  "column", &self->column_order },
		{ DECODE_BOOL, "asc",    &self->asc          },
		{ 0,            NULL,     NULL               },
	};
	decode_obj(obj, "key", pos);
}

static inline void
key_write(Key* self, Buf* buf, int flags)
{
	unused(flags);
	encode_obj(buf);

	// column
	encode_raw(buf, "column", 6);
	encode_int(buf, self->column_order);

	// asc
	encode_raw(buf, "asc", 3);
	encode_bool(buf, self->asc);

	encode_obj_end(buf);
}
