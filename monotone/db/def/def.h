#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Def Def;

struct Def
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
	// row
	int      reserved;
};

static inline Column*
def_column_of(Def* self, int pos)
{
	assert(pos < self->column_count);
	return self->column_index[pos];
}

static inline void
def_init(Def* self)
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
	self->reserved     = 0;
}

static inline void
def_free(Def* self)
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
def_set_reserved(Def* self, int size)
{
	self->reserved = size;
}

static inline void
def_add_column(Def* self, Column* column)
{
	Column** column_index;
	column_index = mn_realloc(self->column_index,
	                          sizeof(Column*) * (self->column_count + 1));
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
def_add_key(Def* self, Column* column)
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
def_find_column(Def* self, Str* name)
{
	auto column = self->column;
	for (; column; column = column->next)
		if (str_compare(&column->name, name))
			return column;
	return NULL;
}

hot static inline Column*
def_find_key(Def* self, Str* name)
{
	auto column = self->key;
	for (; column; column = column->next_key)
		if (str_compare(&column->name, name))
			return column;
	return NULL;
}

hot static inline Column*
def_find_key_by_order(Def* self, int order)
{
	auto column = self->key;
	for (; column; column = column->next_key)
		if (column->order == order)
			return column;
	return NULL;
}

static inline void
def_copy(Def* self, Def* src)
{
	// add columns
	auto column = src->column;
	for (; column; column = column->next)
	{
		auto copy = column_copy(column);
		def_add_column(self, copy);
	}

	// add keys
	column = src->key;
	for (; column; column = column->next_key)
	{
		auto key = def_find_column(self, &column->name);
		assert(key);
		def_add_key(self, key);
	}
	self->key_exclude = src->key_exclude;
	self->key_unique  = src->key_unique;
	self->reserved    = src->reserved;
}

static inline void
def_read(Def* self, uint8_t** pos)
{
	// { column:[], key_unique, key_exclude, key:[], reserved }
	int count;
	data_read_map(pos, &count);

	// column
	data_skip(pos);
	data_read_array(pos, &count);
	int i = 0;
	for (; i < count; i++)
	{
		auto column = column_read(pos);
		def_add_column(self, column);
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
		auto key = def_find_column(self, &name);
		if (unlikely(key == NULL))
			error("key column is not found");
		def_add_key(self, key);
	}

	// reserved
	data_skip(pos);
	int64_t reserved;
	data_read_integer(pos, &reserved);
	self->reserved = reserved;
}

static inline void
def_write(Def* self, Buf* buf)
{
	// { column:[], key_unique, key_exclude, key:[], reserved }
	encode_map(buf, 5);

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

	// reserved
	encode_raw(buf, "reserved", 8);
	encode_integer(buf, self->reserved);
}
