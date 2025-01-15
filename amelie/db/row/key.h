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
	int64_t ref;
	List    link;
};

static inline Key*
key_allocate(void)
{
	Key* self = am_malloc(sizeof(Key));
	self->order  = -1;
	self->column = NULL;
	list_init(&self->link);
	return self;
}

static inline void
key_free(Key* self)
{
	am_free(self);
}

static inline void
key_set_ref(Key* self, int value)
{
	self->ref = value;
}

static inline Key*
key_copy(Key* self)
{
	auto copy = key_allocate();
	key_set_ref(copy, self->ref);
	return copy;
}

static inline Key*
key_read(uint8_t** pos)
{
	auto self = key_allocate();
	errdefer(key_free, self);
	Decode obj[] =
	{
		{ DECODE_INT, "column", &self->ref  },
		{ 0,           NULL,    NULL        },
	};
	decode_obj(obj, "key", pos);
	return self;
}

static inline void
key_write(Key* self, Buf* buf)
{
	encode_obj(buf);

	// column
	encode_raw(buf, "column", 6);
	encode_integer(buf, self->ref);

	encode_obj_end(buf);
}
