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

typedef struct Key    Key;
typedef struct Column Column;

struct Column
{
	int        order;
	Str        name;
	int64_t    type;
	Constraint constraint;
	bool       key;
	List       link;
};

static inline Column*
column_allocate(void)
{
	Column* self = am_malloc(sizeof(Column));
	self->order = 0;
	self->key   = false; 
	self->type  = -1;
	list_init(&self->link);
	str_init(&self->name);
	constraint_init(&self->constraint);
	return self;
}

static inline void
column_free(Column* self)
{
	str_free(&self->name);
	constraint_free(&self->constraint);
	am_free(self);
}

static inline void
column_set_name(Column* self, Str* name)
{
	str_free(&self->name);
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
	column_set_name(copy, &self->name);
	column_set_type(copy, self->type);
	constraint_copy(&self->constraint, &copy->constraint);
	return copy;
}

static inline Column*
column_read(uint8_t** pos)
{
	auto self = column_allocate();
	guard(column_free, self);
	Str type;
	str_init(&type);
	uint8_t* constraints = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING,      "name",       &self->name  },
		{ DECODE_STRING_READ, "type",       &type        },
		{ DECODE_OBJ,         "constraint", &constraints },
		{ 0,                   NULL,        NULL         },
	};
	decode_obj(obj, "columns", pos);
	self->type = type_read(&type);
	constraint_read(&self->constraint, &constraints);
	return unguard();
}

static inline void
column_write(Column* self, Buf* buf)
{
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_cstr(buf, type_of(self->type));

	// constraint
	encode_raw(buf, "constraint", 10);
	constraint_write(&self->constraint, buf);

	encode_obj_end(buf);
}
