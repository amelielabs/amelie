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

enum
{
	IDENTITY_NONE,
	IDENTITY_SERIAL,
	IDENTITY_RANDOM
};

struct Constraints
{
	bool    not_null;
	int64_t as_identity;
	int64_t as_identity_modulo;
	Buf     value;
};

static inline void
constraints_init(Constraints* self)
{
	self->not_null           = false;
	self->as_identity        = IDENTITY_NONE;
	self->as_identity_modulo = INT64_MAX;
	buf_init(&self->value);
}

static inline void
constraints_free(Constraints* self)
{
	buf_free(&self->value);
}

static inline void
constraints_set_not_null(Constraints* self, bool value)
{
	self->not_null = value;
}

static inline void
constraints_set_as_identity(Constraints* self, int value)
{
	self->as_identity = value;
}

static inline void
constraints_set_as_identity_modulo(Constraints* self, int64_t value)
{
	self->as_identity_modulo = value;
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
	constraints_set_as_identity(copy, self->as_identity);
	constraints_set_as_identity_modulo(copy, self->as_identity_modulo);
	constraints_set_default(copy, &self->value);
}

static inline void
constraints_read(Constraints* self, uint8_t** pos)
{
	// [[name, value], ...]
	unpack_array(pos);
	while (! unpack_array_end(pos))
	{
		unpack_array(pos);

		// name
		Str name;
		unpack_str(pos, &name);

		if (str_is_case(&name, "not_null", 8))
			unpack_bool(pos, &self->not_null);
		else
		if (str_is_case(&name, "as_identity", 11))
			unpack_int(pos, &self->as_identity);
		else
		if (str_is_case(&name, "as_identity_modulo", 18))
			unpack_int(pos, &self->as_identity_modulo);
		else
		if (str_is_case(&name, "default", 7))
		{
			Str str;
			unpack_str(pos, &str);
			buf_reset(&self->value);
			base64url_decode(&self->value, &str);
		} else {
			error("unrecognized constraint {str}", &name);
		}

		unpack_array_end(pos);
	}
}

static inline void
constraints_write(Constraints* self, Buf* buf, int flags)
{
	encode_array(buf);

	// not_null
	if (self->not_null)
	{
		encode_array(buf);
		encode_raw(buf, "not_null", 8);
		encode_bool(buf, self->not_null);
		encode_array_end(buf);
	}

	// as_identity
	if (self->as_identity)
	{
		encode_array(buf);
		encode_raw(buf, "as_identity", 11);
		encode_int(buf, self->as_identity);
		encode_array_end(buf);
	}

	// as_identity_modulo
	if (self->as_identity_modulo != INT64_MAX)
	{
		encode_array(buf);
		encode_raw(buf, "as_identity_modulo", 18);
		encode_int(buf, self->as_identity_modulo);
		encode_array_end(buf);
	}

	// default
	if (! buf_empty(&self->value))
	{
		encode_array(buf);
		encode_raw(buf, "default", 7);
		if (flags_has(flags, FMETRICS))
			encode_bool(buf, true);
		else
			encode_base64(buf, &self->value);
		encode_array_end(buf);
	}

	encode_array_end(buf);
}
