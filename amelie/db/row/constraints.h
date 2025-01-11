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

typedef struct Constraints Constraints;

struct Constraints
{
	bool    not_null;
	bool    random;
	int64_t random_modulo;
	bool    as_identity;
	Str     as_stored;
	Str     as_resolved;
	Buf     value;
};

static inline void
constraints_init(Constraints* self)
{
	self->not_null      = false;
	self->random        = false;
	self->random_modulo = INT64_MAX;
	self->as_identity   = false;
	str_init(&self->as_stored);
	str_init(&self->as_resolved);
	buf_init(&self->value);
}

static inline void
constraints_free(Constraints* self)
{
	str_free(&self->as_stored);
	str_free(&self->as_resolved);
	buf_free(&self->value);
}

static inline void
constraints_set_not_null(Constraints* self, bool value)
{
	self->not_null = value;
}

static inline void
constraints_set_random(Constraints* self, bool value)
{
	self->random = value;
}

static inline void
constraints_set_random_modulo(Constraints* self, int64_t value)
{
	self->random_modulo = value;
}

static inline void
constraints_set_as_identity(Constraints* self, bool value)
{
	self->as_identity = value;
}

static inline void
constraints_set_as_stored(Constraints* self, Str* value)
{
	str_free(&self->as_stored);
	str_copy(&self->as_stored, value);
}

static inline void
constraints_set_as_resolved(Constraints* self, Str* value)
{
	str_free(&self->as_resolved);
	str_copy(&self->as_resolved, value);
}

static inline void
constraints_set_default(Constraints* self, Buf* value)
{
	buf_reset(&self->value);
	buf_write_buf(&self->value, value);
}

static inline void
constraints_set_default_str(Constraints* self, Str* value)
{
	buf_reset(&self->value);
	buf_write_str(&self->value, value);
}

static inline void
constraints_copy(Constraints* self, Constraints* copy)
{
	constraints_set_not_null(copy, self->not_null);
	constraints_set_random(copy, self->random);
	constraints_set_random_modulo(copy, self->random_modulo);
	constraints_set_as_identity(copy, self->as_identity);
	constraints_set_as_stored(copy, &self->as_stored);
	constraints_set_as_resolved(copy, &self->as_resolved);
	constraints_set_default(copy, &self->value);
}

static inline void
constraints_read(Constraints* self, uint8_t** pos)
{
	Decode obj[] =
	{
		{ DECODE_BOOL,   "not_null",      &self->not_null      },
		{ DECODE_BOOL,   "random",        &self->random        },
		{ DECODE_INT,    "random_modulo", &self->random_modulo },
		{ DECODE_DATA,   "default",       &self->value         },
		{ DECODE_BOOL,   "as_identity",   &self->as_identity   },
		{ DECODE_STRING, "as_stored",     &self->as_stored     },
		{ DECODE_STRING, "as_resolved",   &self->as_resolved   },
		{ 0,              NULL,           NULL                 },
	};
	decode_obj(obj, "constraints", pos);
}

static inline void
constraints_write(Constraints* self, Buf* buf)
{
	encode_obj(buf);

	// not_null
	encode_raw(buf, "not_null", 8);
	encode_bool(buf, self->not_null);

	// random
	encode_raw(buf, "random", 6);
	encode_bool(buf, self->random);

	// random_modulo
	encode_raw(buf, "random_modulo", 13);
	encode_integer(buf, self->random_modulo);

	// default
	encode_raw(buf, "default", 7);
	buf_write(buf, self->value.start, buf_size(&self->value));

	// as_identity
	encode_raw(buf, "as_identity", 11);
	encode_bool(buf, self->as_identity);

	// as_stored
	encode_raw(buf, "as_stored", 9);
	encode_string(buf, &self->as_stored);

	// as_resolved
	encode_raw(buf, "as_resolved", 11);
	encode_string(buf, &self->as_resolved);

	encode_obj_end(buf);
}
