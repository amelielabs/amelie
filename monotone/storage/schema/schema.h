#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Schema Schema;

struct Schema
{
	// columns
	Column*  column;
	Column*  column_tail;
	Column** column_index;
	int      column_count;
	// keys
	Column*  key;
	Column*  key_tail;
	int      key_count;
	int      key_exclude;
	bool     key_unique;
};

static inline Column*
schema_column_of(Schema* self, int pos)
{
	assert(pos < self->column_count);
	return self->column_index[pos];
}

static inline void
schema_init(Schema* self)
{
	self->column       = NULL;
	self->column_tail  = NULL;
	self->column_index = NULL;
	self->column_count = 0;
	self->key          = NULL;
	self->key_tail     = NULL;
	self->key_count    = 0;
	self->key_exclude  = 0;
	self->key_unique   = false;
}

static inline void
schema_free(Schema* self)
{
	auto column = self->column;
	while (column)
	{
		auto next = column->next;
		column_free(column);
		column = next;
	}
	if (self->column_index)
		mn_free(self->column_index);
}

static inline void
schema_add_column(Schema* self, Column* column)
{
	Column** column_index;
	column_index = mn_realloc(self->column_index, sizeof(Column*) * (self->column_count + 1));

	column->order = self->column_count;
	if (self->column == NULL)
		self->column = column;
	else
		self->column_tail->next = column;
	self->column_tail = column;
	self->column_count++;
	self->column_index = column_index;
	self->column_index[self->column_count - 1] = column;
}

static inline void
schema_add_key(Schema* self, Column* column)
{
	assert(column->order_key == -1);
	column->order_key = self->key_count;
	if (self->key == NULL)
		self->key = column;
	else
		self->key_tail->next_key = column;
	self->key_tail = column;
	self->key_count++;
}

hot static inline Column*
schema_find_column(Schema* self, Str* name)
{
	auto column = self->column;
	for (; column; column = column->next)
		if (str_compare(&column->name, name))
			return column;
	return NULL;
}

hot static inline Column*
schema_find_key(Schema* self, Str* name)
{
	auto column = self->key;
	for (; column; column = column->next_key)
		if (str_compare(&column->name, name))
			return column;
	return NULL;
}

hot static inline Column*
schema_find_key_by_order(Schema* self, int order)
{
	auto column = self->key;
	for (; column; column = column->next_key)
		if (column->order == order)
			return column;
	return NULL;
}

static inline void
schema_copy(Schema* self, Schema* src)
{
	// add columns
	auto column = src->column;
	for (; column; column = column->next)
	{
		auto copy = column_copy(column);
		schema_add_column(self, copy);
	}

	// add keys
	column = src->key;
	for (; column; column = column->next_key)
	{
		auto key = schema_find_column(self, &column->name);
		assert(key);
		schema_add_key(self, key);
	}
	self->key_exclude = src->key_exclude;
	self->key_unique  = src->key_unique;
}

static inline void
schema_read(Schema* self, uint8_t** pos)
{
	// { column:[], key_unique, key_exclude, key:[] }
	int count;
	data_read_map(pos, &count);

	// column
	data_skip(pos);
	data_read_array(pos, &count);
	int i = 0;
	for (; i < count; i++)
	{
		auto column = column_read(pos);
		schema_add_column(self, column);
	}

	// key_unique
	data_skip(pos);
	data_read_bool(pos, &self->key_unique);

	// key_exclude
	data_skip(pos);
	int64_t key_exclude;
	data_read_integer(pos, &key_exclude);
	self->key_exclude = key_exclude;

	// key
	data_skip(pos);
	data_read_array(pos, &count);
	i = 0;
	for (; i < count; i++)
	{
		Str name;
		data_read_string(pos, &name);
		auto key = schema_find_column(self, &name);
		if (unlikely(key == NULL))
			error("schema column is not found");
		schema_add_key(self, key);
	}
}

static inline void
schema_write(Schema* self, Buf* buf)
{
	// { column:[], key_unique, key_exclude, key:[] }
	encode_map(buf, 4);

	// column
	encode_raw(buf, "column", 6);
	encode_array(buf, self->column_count);
	auto column = self->column;
	for (; column; column = column->next)
		column_write(column, buf);

	// key_unique
	encode_raw(buf, "key_unique", 10);
	encode_bool(buf, self->key_unique);

	// key_exclude
	encode_raw(buf, "key_exclude", 11);
	encode_integer(buf, self->key_exclude);

	// key
	encode_raw(buf, "key", 3);
	encode_array(buf, self->key_count);
	column = self->key;
	for (; column; column = column->next_key)
		encode_string(buf, &column->name);
}
