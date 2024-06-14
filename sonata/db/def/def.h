#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	bool     key_unique;
	// secondary
	Def*     primary;
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
	self->key_unique   = false;
	self->primary      = NULL;
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
		so_free(self->column_index);

	auto key = self->key;
	while (key)
	{
		auto next = key->next;
		key_free(key);
		key = next;
	}
}

static inline void
def_add_column(Def* self, Column* column)
{
	Column** column_index;
	column_index = so_realloc(self->column_index,
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
	self->key_unique = src->key_unique;
}

static inline void
def_read(Def* self, uint8_t** pos)
{
	// { column:[], key[], key_unique }
	uint8_t* pos_column = NULL;
	uint8_t* pos_key = NULL;
	Decode map[] =
	{
		{ DECODE_ARRAY, "column",     &pos_column        },
		{ DECODE_ARRAY, "key",        &pos_key           },
		{ DECODE_BOOL,  "key_unique", &self->key_unique  },
		{ 0,             NULL,        NULL               },
	};
	decode_map(map, pos);

	// column
	data_read_array(&pos_column);
	while (! data_read_array_end(&pos_column))
	{
		auto column = column_read(&pos_column);
		def_add_column(self, column);
	}

	// key
	data_read_array(&pos_key);
	while (! data_read_array_end(&pos_key))
	{
		auto key = key_read(&pos_key);
		def_add_key(self, key);
	}
}

static inline void
def_write(Def* self, Buf* buf)
{
	// { column:[], key:[], key_unique }
	encode_map(buf);

	// column
	encode_raw(buf, "column", 6);
	encode_array(buf);
	auto column = self->column;
	for (; column; column = column->next)
		column_write(column, buf);
	encode_array_end(buf);

	// key
	encode_raw(buf, "key", 3);
	encode_array(buf);
	auto key = self->key;
	for (; key; key = key->next)
		key_write(key, buf);
	encode_array_end(buf);

	// key_unique
	encode_raw(buf, "key_unique", 10);
	encode_bool(buf, self->key_unique);

	encode_map_end(buf);
}
