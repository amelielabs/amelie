#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Keys Keys;

struct Keys
{
	List     list;	
	int      list_count;
	int      key_size;
	bool     primary;
	Columns* columns;
};

static inline void
keys_init(Keys* self, Columns* columns)
{
	self->list_count = 0;
	self->key_size   = 0;
	self->primary    = false;
	self->columns    = columns;
	list_init(&self->list);
}

static inline void
keys_free(Keys* self)
{
	list_foreach_safe(&self->list)
	{
		auto key = list_at(Key, link);
		key_free(key);
	}
}

static inline void
keys_set(Keys* self, bool primary, int key_size)
{
	self->primary  = primary;
	self->key_size = key_size;
}

static inline void
keys_add(Keys* self, Key* key)
{
	assert(key->order == -1);

	// add key
	key->order = self->list_count;
	list_append(&self->list, &key->link);
	self->list_count++;

	// set column pointer
	key->column = columns_of(self->columns, key->ref);
	key->column->key = true;
}

static inline void
keys_update(Keys* self)
{
	int order = 0;
	list_foreach_safe(&self->list)
	{
		auto key = list_at(Key, link);
		key->order = order;
		key->ref = key->column->order;
		order++;
	}
}

hot static inline Key*
keys_find(Keys* self, Str* name)
{
	list_foreach_safe(&self->list)
	{
		auto key = list_at(Key, link);
		if (str_compare(&key->column->name, name))
			return key;
	}
	return NULL;
}

static inline void
keys_copy(Keys* self, Keys* src)
{
	list_foreach_safe(&src->list)
	{
		auto copy = key_copy(list_at(Key, link));
		keys_add(self, copy);
	}
}

static inline void
keys_copy_distinct(Keys* self, Keys* primary)
{
	// add keys which are not already present
	list_foreach_safe(&primary->list)
	{
		auto key = list_at(Key, link);
		auto key_primary = keys_find(self, &key->column->name);
		if (key_primary && key_compare(key, key_primary))
			continue;
		auto copy = key_copy(list_at(Key, link));
		keys_add(self, copy);
	}
}

static inline void
keys_read(Keys* self, uint8_t** pos)
{
	// []
	data_read_array(pos);
	while (! data_read_array_end(pos))
	{
		auto key = key_read(pos);
		keys_add(self, key);
	}
}

static inline void
keys_write(Keys* self, Buf* buf)
{
	// []
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto key = list_at(Key, link);
		key_write(key, buf);
	}
	encode_array_end(buf);
}
