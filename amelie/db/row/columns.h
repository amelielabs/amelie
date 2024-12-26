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
	List    list_variable;
	int     count;
	int     count_variable;
	int     count_stored;
	int     count_resolved;
	int     precomputed;
	Column* serial;
};

static inline void
columns_init(Columns* self)
{
	self->count          = 0;
	self->count_stored   = 0;
	self->count_resolved = 0;
	self->precomputed    = 0;
	self->serial         = NULL;
	list_init(&self->list_variable);
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
}

static inline void
columns_add(Columns* self, Column* column)
{
	list_append(&self->list, &column->link);
	column->order = self->count;
	self->count++;

	if (column->type_size) {
		self->precomputed += column->type_size;
	} else
	{
		list_append(&self->list_variable, &column->link_variable);
		self->count_variable++;
	}

	if (! str_empty(&column->constraints.as_stored))
		self->count_stored++;
	if (! str_empty(&column->constraints.as_resolved))
		self->count_resolved++;

	// save order of the first serial column
	if (column->constraints.serial && !self->serial)
		self->serial = column;
}

static inline void
columns_del(Columns* self, Column* column)
{
	list_unlink(&column->link);
	self->count--;
	assert(self->count >= 0);

	if (column->type_size) {
		self->precomputed -= column->type_size;
	} else
	{
		list_unlink(&column->link_variable);
		self->count_variable--;
	}

	if (! str_empty(&column->constraints.as_stored))
		self->count_stored--;
	if (! str_empty(&column->constraints.as_resolved))
		self->count_resolved--;

	if (self->serial == column)
		self->serial = NULL;

	// reorder columns
	int order = 0;
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		column->order = order++;
		if (column->constraints.serial && !self->serial)
			self->serial = column;
	}
}

static inline void
columns_sync(Columns* self)
{
	// sync generated columns count
	self->count_stored   = 0;
	self->count_resolved = 0;
	list_foreach_safe(&self->list)
	{
		auto column = list_at(Column, link);
		if (! str_empty(&column->constraints.as_stored))
			self->count_stored++;
		if (! str_empty(&column->constraints.as_resolved))
			self->count_resolved++;
	}
}

hot static inline Column*
columns_find(Columns* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
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
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		auto column = column_read(pos);
		columns_add(self, column);
	}
}

static inline void
columns_write(Columns* self, Buf* buf)
{
	// []
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		column_write(column, buf);
	}
	encode_array_end(buf);
}
