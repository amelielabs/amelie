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
	int         order;
	Str         name;
	int64_t     type;
	int64_t     type_size;
	Constraints constraints;
	int         refs;
	List        link_variable;
	List        link;
};

static inline Column*
column_allocate(void)
{
	Column* self = am_malloc(sizeof(Column));
	self->order     = 0;
	self->refs      = 0;
	self->type      = -1;
	self->type_size = 0;
	list_init(&self->link);
	list_init(&self->link_variable);
	str_init(&self->name);
	constraints_init(&self->constraints);
	return self;
}

static inline void
column_free(Column* self)
{
	str_free(&self->name);
	constraints_free(&self->constraints);
	am_free(self);
}

static inline void
column_set_name(Column* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
column_set_type(Column* self, int type, int type_size)
{
	self->type = type;
	self->type_size = type_size;
}

static inline Column*
column_copy(Column* self)
{
	auto copy = column_allocate();
	column_set_name(copy, &self->name);
	column_set_type(copy, self->type, self->type_size);
	constraints_copy(&self->constraints, &copy->constraints);
	return copy;
}

static inline Column*
column_read(uint8_t** pos)
{
	auto self = column_allocate();
	errdefer(column_free, self);
	Str type;
	str_init(&type);
	uint8_t* constraints = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "name",        &self->name      },
		{ DECODE_INT,    "type",        &self->type      },
		{ DECODE_INT,    "type_size",   &self->type_size },
		{ DECODE_OBJ,    "constraints", &constraints     },
		{ 0,              NULL,         NULL             },
	};
	decode_obj(obj, "columns", pos);
	constraints_read(&self->constraints, &constraints);
	return self;
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
	encode_integer(buf, self->type);

	// type_size
	encode_raw(buf, "type_size", 9);
	encode_integer(buf, self->type_size);

	// constraints
	encode_raw(buf, "constraints", 11);
	constraints_write(&self->constraints, buf);

	encode_obj_end(buf);
}
