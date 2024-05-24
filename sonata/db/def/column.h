#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Key    Key;
typedef struct Column Column;

struct Column
{
	Str        name;
	int        order;
	int64_t    type;
	Constraint constraint;
	Key*       key;
	Key*       key_tail;
	Column*    next;
};

static inline Column*
column_allocate(void)
{
	Column* self = so_malloc(sizeof(Column));
	self->order    = 0;
	self->type     = -1;
	self->key      = NULL;
	self->key_tail = NULL;
	self->next     = NULL;
	str_init(&self->name);
	constraint_init(&self->constraint);
	return self;
}

static inline void
column_free(Column* self)
{
	str_free(&self->name);
	constraint_free(&self->constraint);
	so_free(self);
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
	guard(column_free, copy);
	column_set_name(copy, &self->name);
	column_set_type(copy, self->type);
	constraint_copy(&self->constraint, &copy->constraint);
	return unguard();
}

static inline Column*
column_read(uint8_t** pos)
{
	auto self = column_allocate();
	guard(column_free, self);
	uint8_t* constraints = NULL;
	Decode map[] =
	{
		{ DECODE_STRING, "name",       &self->name  },
		{ DECODE_INT,    "type",       &self->type  },
		{ DECODE_MAP,    "constraint", &constraints },
		{ 0,              NULL,        NULL         },
	};
	decode_map(map, pos);
	constraint_read(&self->constraint, &constraints);
	return unguard();
}

static inline void
column_write(Column* self, Buf* buf)
{
	encode_map(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// constraint
	encode_raw(buf, "constraint", 10);
	constraint_write(&self->constraint, buf);

	encode_map_end(buf);
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
		error("column <%.*s>: does not match data type",
		      str_size(&self->name),
		      str_of(&self->name));
}
