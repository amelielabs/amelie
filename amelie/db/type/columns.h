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

typedef struct Columns Columns;

struct Columns
{
	List    list;
	int     count;
	int     count_flat;
	Column* identity;
};

static inline void
columns_init(Columns* self)
{
	self->count      = 0;
	self->count_flat = 0;
	self->identity   = NULL;
	list_init(&self->list);
}

static inline void
columns_free(Columns* self)
{
	list_foreach_safe(&self->list)
	{
		auto column = list_at(Column, link);
		column_free(column);
	}
	columns_init(self);
}

static inline void
columns_add(Columns* self, Column* column)
{
	list_append(&self->list, &column->link);
	column->order = self->count;
	self->count++;

	if (column->type == TYPE_VECTOR)
	{
		column->order_flat = self->count_flat;
		self->count_flat++;
	}

	// save order of the first identity column
	auto cons = &column->constraints;
	if (cons->as_identity && !self->identity)
		self->identity = column;
}

static inline void
columns_del(Columns* self, Column* column)
{
	list_unlink(&column->link);
	self->count--;
	assert(self->count >= 0);

	// note: assuming del only used for abort (last column)
	if (column->type == TYPE_VECTOR)
		self->count_flat--;

	if (self->identity == column)
		self->identity = NULL;
}

static inline void
columns_sync(Columns* self)
{
	// sync generated columns count
	self->identity = NULL;
	list_foreach_safe(&self->list)
	{
		auto column = list_at(Column, link);
		auto cons = &column->constraints;
		if (cons->as_identity && !self->identity)
			self->identity = column;
	}
}

hot static inline Column*
columns_find(Columns* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		if (column->dropped)
			continue;
		if (str_compare(&column->name, name))
			return column;
	}
	return NULL;
}

hot static inline Column*
columns_find_noconflict(Columns* self, Str* name, bool* conflict)
{
	Column* match = NULL;
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		if (column->dropped)
			continue;
		if (str_compare(&column->name, name))
		{
			if (match)
			{
				*conflict = true;
				return NULL;
			}
			match = column;
		}
	}
	return match;
}

hot static inline Column*
columns_find_by(Columns* self, int order)
{
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		if (column->order == order)
			return column;
	}
	return NULL;
}

hot static inline Column*
columns_first(Columns* self)
{
	return container_of(self->list.next, Column, link);
}

hot static inline bool
columns_compare(Columns* self, Columns* with)
{
	if (self->count != with->count)
		return false;
	auto with_it = with->list.next;
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		auto column_with = container_of(with_it, Column, link);
		if (column->type != column_with->type)
			return false;
		with_it = with_it->next;
	}
	return true;
}

static inline void
columns_copy_types(Columns* self, Columns* src)
{
	assert(self->count == src->count);
	auto src_it = src->list.next;
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		auto column_src = container_of(src_it, Column, link);
		column->type = column_src->type;
		src_it = src_it->next;
	}
}

static inline void
columns_copy(Columns* self, Columns* src)
{
	list_foreach(&src->list)
	{
		auto copy = column_copy(list_at(Column, link));
		columns_add(self, copy);
	}
}

static inline void
columns_read(Columns* self, uint8_t** pos)
{
	// []
	unpack_array(pos);
	while (! unpack_array_end(pos))
	{
		auto column = column_read(pos);
		columns_add(self, column);
	}
}

static inline void
columns_write(Columns* self, Buf* buf, int flags)
{
	// []
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		column_write(column, buf, flags);
	}
	encode_array_end(buf);
}
