#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Columns Columns;

struct Columns
{
	Column** index;
	List     list;
	int      list_count;
};

static inline Column*
columns_of(Columns* self, int pos)
{
	assert(pos < self->list_count);
	return self->index[pos];
}

static inline void
columns_init(Columns* self)
{
	self->index      = NULL;
	self->list_count = 0;
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
	if (self->index)
		am_free(self->index);
}

static inline void
columns_add(Columns* self, Column* column)
{
	Column** index;
	index = am_realloc(self->index, sizeof(Column*) * (self->list_count + 1));
	column->order = self->list_count;
	list_append(&self->list, &column->link);
	self->list_count++;
	self->index = index;
	self->index[self->list_count - 1] = column;
}

static inline void
columns_del(Columns* self, int at)
{
	assert(self->list_count > 1);
	Column** index;
	index = am_realloc(self->index, sizeof(Column*) * (self->list_count - 1));
	int order = 0;
	list_foreach_safe(&self->list)
	{
		auto column = list_at(Column, link);
		if (column->order == at)
		{
			list_unlink(&column->link);
			self->list_count--;
			column_free(column);
			continue;
		}
		column->order = order;
		index[order] = column;
		order++;
	}
	self->index = index;
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
	data_read_array(pos);
	while (! data_read_array_end(pos))
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
