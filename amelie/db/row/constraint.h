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

typedef struct Constraint Constraint;

enum
{
	GENERATED_NONE,
	GENERATED_SERIAL,
	GENERATED_RANDOM
};

struct Constraint
{
	bool    not_null;
	bool    not_aggregated;
	int64_t generated;
	int64_t modulo;
	int64_t aggregate;
	Str     as_stored;
	Str     as_aggregated;
	Buf     value;
};

static inline void
constraint_init(Constraint* self)
{
	self->not_null       = false;
	self->not_aggregated = true;
	self->generated      = GENERATED_NONE;
	self->modulo         = INT64_MAX;
	self->aggregate      = AGG_UNDEF;
	str_init(&self->as_stored);
	str_init(&self->as_aggregated);
	buf_init(&self->value);
}

static inline void
constraint_free(Constraint* self)
{
	str_free(&self->as_stored);
	str_free(&self->as_aggregated);
	buf_free(&self->value);
}

static inline void
constraint_set_not_null(Constraint* self, bool value)
{
	self->not_null = value;
}

static inline void
constraint_set_not_aggregated(Constraint* self, bool value)
{
	self->not_aggregated = value;
}

static inline void
constraint_set_generated(Constraint* self, int value)
{
	self->generated = value;
}

static inline void
constraint_set_modulo(Constraint* self, int64_t value)
{
	self->modulo = value;
}

static inline void
constraint_set_aggregate(Constraint* self, int64_t value)
{
	self->aggregate = value;
}

static inline void
constraint_set_as_stored(Constraint* self, Str* value)
{
	str_free(&self->as_stored);
	str_copy(&self->as_stored, value);
}

static inline void
constraint_set_as_aggregated(Constraint* self, Str* value)
{
	str_free(&self->as_aggregated);
	str_copy(&self->as_aggregated, value);
}

static inline void
constraint_set_default(Constraint* self, Buf* value)
{
	buf_reset(&self->value);
	buf_write(&self->value, value->start, buf_size(value));
}

static inline void
constraint_copy(Constraint* self, Constraint* copy)
{
	constraint_set_not_null(copy, self->not_null);
	constraint_set_not_aggregated(copy, self->not_aggregated);
	constraint_set_generated(copy, self->generated);
	constraint_set_modulo(copy, self->modulo);
	constraint_set_aggregate(copy, self->aggregate);
	constraint_set_as_stored(copy, &self->as_stored);
	constraint_set_as_aggregated(copy, &self->as_aggregated);
	constraint_set_default(copy, &self->value);
}

static inline void
constraint_read(Constraint* self, uint8_t** pos)
{
	Decode obj[] =
	{
		{ DECODE_BOOL,   "not_null",       &self->not_null       },
		{ DECODE_BOOL,   "not_aggregated", &self->not_aggregated },
		{ DECODE_INT,    "generated",      &self->generated      },
		{ DECODE_INT,    "modulo",         &self->modulo         },
		{ DECODE_INT,    "aggregate",      &self->aggregate      },
		{ DECODE_STRING, "as_stored",      &self->as_stored      },
		{ DECODE_STRING, "as_aggregated",  &self->as_aggregated  },
		{ DECODE_DATA,   "default",        &self->value          },
		{ 0,              NULL,            NULL                  },
	};
	decode_obj(obj, "constraint", pos);
}

static inline void
constraint_write(Constraint* self, Buf* buf)
{
	encode_obj(buf);

	// not_null
	encode_raw(buf, "not_null", 8);
	encode_bool(buf, self->not_null);

	// not_aggregated
	encode_raw(buf, "not_aggregated", 14);
	encode_bool(buf, self->not_aggregated);

	// generated
	encode_raw(buf, "generated", 9);
	encode_integer(buf, self->generated);

	// modulo
	encode_raw(buf, "modulo", 6);
	encode_integer(buf, self->modulo);

	// aggregate
	encode_raw(buf, "aggregate", 9);
	encode_integer(buf, self->aggregate);

	// as_stored
	encode_raw(buf, "as_stored", 9);
	encode_string(buf, &self->as_stored);

	// as_aggregated
	encode_raw(buf, "as_aggregated", 13);
	encode_string(buf, &self->as_aggregated);

	// default
	encode_raw(buf, "default", 7);
	buf_write(buf, self->value.start, buf_size(&self->value));

	encode_obj_end(buf);
}
