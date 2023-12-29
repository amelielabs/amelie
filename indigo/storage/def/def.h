#pragma once

//
// indigo
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
	Key*     key;
	Key*     key_tail;
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

	auto key = self->key;
	while (key)
	{
		auto next = key->next;
		key_free(key);
		key = next;
	}
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
def_add_key(Def* self, Key* key)
{
	assert(key->order == -1);

	// add key
	key->order = self->key_count;
	if (self->key == NULL)
		self->key = key;
	else
		self->key_tail->next = key;
	self->key_tail = key;
	self->key_count++;

	// add key to the column
	auto column = def_column_of(self, key->column);
	if (column->key == NULL)
		column->key = key;
	else
		column->key_tail->next_column = key;
	column->key_tail = key;
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

hot static inline Key*
def_find_key(Def* self, Str* name)
{
	auto key = self->key;
	for (; key; key = key->next)
	{
		auto column = def_column_of(self, key->column);
		if (str_compare(&column->name, name))
			return key;
	}
	return NULL;
}

hot static inline Key*
def_find_column_key(Column* self, Str* path)
{
	auto key = self->key;
	for (; key; key = key->next_column)
	{
		if (str_compare(&key->path, path))
			return key;
	}
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
	auto key = src->key;
	for (; key; key = key->next)
	{
		auto copy = key_copy(key);
		def_add_key(self, copy);
	}
	self->key_exclude = src->key_exclude;
	self->key_unique  = src->key_unique;
	self->reserved    = src->reserved;
}

static inline void
def_read(Def* self, uint8_t** pos)
{
	// { column:[], key[], key_unique, key_exclude, reserved }
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

	// key
	data_skip(pos);
	data_read_array(pos, &count);
	i = 0;
	for (; i < count; i++)
	{
		auto key = key_read(pos);
		def_add_key(self, key);
	}

	// key_unique
	data_skip(pos);
	data_read_bool(pos, &self->key_unique);

	// key_exclude
	data_skip(pos);
	int64_t key_exclude;
	data_read_integer(pos, &key_exclude);
	self->key_exclude = key_exclude;

	// reserved
	data_skip(pos);
	int64_t reserved;
	data_read_integer(pos, &reserved);
	self->reserved = reserved;
}

static inline void
def_write(Def* self, Buf* buf)
{
	// { column:[], key:[], key_unique, key_exclude, reserved }
	encode_map(buf, 5);

	// column
	encode_raw(buf, "column", 6);
	encode_array(buf, self->column_count);
	auto column = self->column;
	for (; column; column = column->next)
		column_write(column, buf);

	// key
	encode_raw(buf, "key", 3);
	encode_array(buf, self->key_count);
	auto key = self->key;
	for (; key; key = key->next)
		key_write(key, buf);

	// key_unique
	encode_raw(buf, "key_unique", 10);
	encode_bool(buf, self->key_unique);

	// key_exclude
	encode_raw(buf, "key_exclude", 11);
	encode_integer(buf, self->key_exclude);

	// reserved
	encode_raw(buf, "reserved", 8);
	encode_integer(buf, self->reserved);
}
