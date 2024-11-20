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
	List list;
	int  list_count;
	int  generated_columns;
};

static inline void
columns_init(Columns* self)
{
	self->list_count = 0;
	self->generated_columns = 0;
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
	column->order = self->list_count;
	list_append(&self->list, &column->link);
	self->list_count++;

	if (! str_empty(&column->constraint.as_stored))
		self->generated_columns++;
}

static inline void
columns_del(Columns* self, Column* column)
{
	list_unlink(&column->link);
	self->list_count--;
	assert(self->list_count >= 0);

	// reorder columns
	int order = 0;
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		column->order = order++;
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
