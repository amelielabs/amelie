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

typedef struct Keys Keys;

struct Keys
{
	Comparable comparable;
	Buf        list;
	int        count;
	Columns*   columns;
};

always_inline static inline Key*
keys_at(Keys* self, int order)
{
	return &((Key*)self->list.start)[order];
}

static inline void
keys_init(Keys* self, Columns* columns)
{
	self->columns = columns;
	self->count   = 0;
	buf_init(&self->list);
	comparable_init(&self->comparable);
}

static inline void
keys_free(Keys* self)
{
	for (auto at = 0; at < self->count; at++)
	{
		auto key = keys_at(self, at);
		key->column->refs--;
	}
	buf_free(&self->list);
	comparable_free(&self->comparable);
}

static inline Key*
keys_add(Keys* self, int column_order, bool asc, bool partitioning)
{
	// add key
	auto key = (Key*)buf_emplace(&self->list, sizeof(Key));
	key_init(key);
	key->order        = self->count;
	key->column_order = column_order;
	key->asc          = asc;
	key->partitioning = partitioning;
	self->count++;

	// resolve column
	key->column = columns_find_by(self->columns, column_order);
	key->column->refs++;

	// add to the comparable
	comparable_add(&self->comparable, key->column);
	return key;
}

hot static inline Key*
keys_find(Keys* self, Str* name)
{
	for (auto at = 0; at < self->count; at++)
	{
		auto key = keys_at(self, at);
		if (str_compare(&key->column->name, name))
			return key;
	}
	return NULL;
}

hot static inline Key*
keys_find_column(Keys* self, int order)
{
	for (auto at = 0; at < self->count; at++)
	{
		auto key = keys_at(self, at);
		if (key->column_order == order)
			return key;
	}
	return NULL;
}

static inline void
keys_copy(Keys* self, Keys* src)
{
	for (auto at = 0; at < src->count; at++)
	{
		auto key = keys_at(src, at);
		keys_add(self, key->column_order, key->asc, key->partitioning);
	}
}

static inline void
keys_copy_distinct(Keys* self, Keys* primary)
{
	for (auto at = 0; at < primary->count; at++)
	{
		auto key = keys_at(primary, at);
		if (keys_find_column(self, key->column_order))
			continue;
		keys_add(self, key->column_order, key->asc, key->partitioning);
	}
}

static inline void
keys_read(Keys* self, uint8_t** pos)
{
	// []
	unpack_array(pos);
	while (! unpack_array_end(pos))
	{
		Key read;
		key_init(&read);
		key_read(&read, pos);
		keys_add(self, read.column_order, read.asc, read.partitioning);
	}
}

static inline void
keys_write(Keys* self, Buf* buf, int flags)
{
	// []
	encode_array(buf);
	for (auto at = 0; at < self->count; at++)
	{
		auto key = keys_at(self, at);
		key_write(key, buf, flags);
	}
	encode_array_end(buf);
}
