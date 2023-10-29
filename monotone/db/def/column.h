#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Key    Key;
typedef struct Column Column;

struct Column
{
	Str     name;
	int     order;
	int64_t type;
	Key*    key;
	Key*    key_tail;
	Column* next;
};

static inline Column*
column_allocate(void)
{
	Column* self = mn_malloc(sizeof(Column));
	self->order    = 0;
	self->type     = -1;
	self->key      = NULL;
	self->key_tail = NULL;
	self->next     = NULL;
	str_init(&self->name);
	return self;
}

static inline void
column_free(Column* self)
{
	str_free(&self->name);
	mn_free(self);
}

static inline void
column_set_name(Column* self, Str* name)
{
	str_copy(&self->name, name);
}

static inline void
column_set_type(Column* self, int value)
{
	self->type = value;
}

static inline Column*
column_copy(Column* self)
{
	auto copy = column_allocate();
	guard(copy_guard, column_free, copy);
	column_set_name(copy, &self->name);
	column_set_type(copy, self->type);
	return unguard(&copy_guard);
}

static inline Column*
column_read(uint8_t** pos)
{
	auto self = column_allocate();
	guard(self_guard, column_free, self);

	int count;
	data_read_map(pos, &count);

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// type
	data_skip(pos);
	data_read_integer(pos, &self->type);

	return unguard(&self_guard);
}

static inline void
column_write(Column* self, Buf* buf)
{
	encode_map(buf, 2);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);
}

hot static inline void
column_find(Column* self, uint8_t** pos)
{
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");

	// find column
	if (! array_find(pos, self->order))
		error("column <%.*s>: is not found", str_size(&self->name),
		      str_of(&self->name));

	// validate data type
	if (! type_validate(self->type, *pos))
	{
		if (data_is_partial(*pos))
			return;
		error("column <%.*s>: does not match data type",
		      str_size(&self->name),
		      str_of(&self->name));
	}
}
