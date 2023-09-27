#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Column Column;

struct Column
{
	Str     name;
	int     order;
	int     order_key;
	int64_t type;
	bool    asc;
	Column* next;
	Column* next_key;
};

static inline Column*
column_allocate(void)
{
	Column* self = mn_malloc(sizeof(Column));
	self->order     = 0;
	self->order_key = -1;
	self->type      = -1;
	self->asc       = false;
	self->next      = NULL;
	self->next_key  = NULL;
	str_init(&self->name);
	return self;
}

static inline void
column_free(Column* self)
{
	str_free(&self->name);
	mn_free(self);
}

static inline bool
column_is_key(Column* self)
{
	return self->order_key != -1;
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

static inline void
column_set_asc(Column* self, bool value)
{
	self->asc = value;
}

static inline Column*
column_copy(Column* self)
{
	auto copy = column_allocate();
	guard(copy_guard, column_free, copy);
	column_set_name(copy, &self->name);
	column_set_type(copy, self->type);
	column_set_asc(copy, self->asc);
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

	// asc
	data_skip(pos);
	data_read_bool(pos, &self->asc);

	return unguard(&self_guard);
}

static inline void
column_write(Column* self, Buf* buf)
{
	encode_map(buf, 3);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// asc
	encode_raw(buf, "asc", 3);
	encode_bool(buf, self->asc);
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
	if (! schema_type_validate(self->type, *pos))
	{
		if (data_is_partial(*pos))
			return;
		error("column <%.*s>: does not match data type",
		      str_size(&self->name),
		      str_of(&self->name));
	}
}
