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
	Str     as_stored;
	Str     as_resolved;
	Buf     value;
};

static inline void
constraints_init(Constraints* self)
{
	self->not_null           = false;
	self->as_identity        = IDENTITY_NONE;
	self->as_identity_modulo = INT64_MAX;
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
	constraints_set_as_identity(copy, self->as_identity);
	constraints_set_as_identity_modulo(copy, self->as_identity_modulo);
	constraints_set_as_stored(copy, &self->as_stored);
	constraints_set_as_resolved(copy, &self->as_resolved);
	constraints_set_default(copy, &self->value);
}

static inline void
constraints_read(Constraints* self, uint8_t** pos)
{
	// [[name, value], ...]
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		json_read_array(pos);

		// name
		Str name;
		json_read_string(pos, &name);

		if (str_is_case(&name, "not_null", 8))
			json_read_bool(pos, &self->not_null);
		else
		if (str_is_case(&name, "as_identity", 11))
			json_read_integer(pos, &self->as_identity);
		else
		if (str_is_case(&name, "as_identity_modulo", 18))
			json_read_integer(pos, &self->as_identity_modulo);
		else
		if (str_is_case(&name, "as_stored", 9))
			json_read_string_copy(pos, &self->as_stored);
		else
		if (str_is_case(&name, "as_resolved", 11))
			json_read_string_copy(pos, &self->as_resolved);
		else
		if (str_is_case(&name, "default", 7))
		{
			Str str;
			json_read_string(pos, &str);
			buf_reset(&self->value);
			base64url_decode(&self->value, &str);
		} else {
			error("unrecognized constraint %.*s", str_size(&name),
			      str_of(&name));
		}

		json_read_array_end(pos);
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
		encode_integer(buf, self->as_identity);
		encode_array_end(buf);
	}

	// as_identity_modulo
	if (self->as_identity_modulo != INT64_MAX)
	{
		encode_array(buf);
		encode_raw(buf, "as_identity_modulo", 18);
		encode_integer(buf, self->as_identity_modulo);
		encode_array_end(buf);
	}

	// as_stored
	if (! str_empty(&self->as_stored))
	{
		encode_array(buf);
		encode_raw(buf, "as_stored", 9);
		encode_string(buf, &self->as_stored);
		encode_array_end(buf);
	}

	// as_resolved
	if (! str_empty(&self->as_resolved))
	{
		encode_array(buf);
		encode_raw(buf, "as_resolved", 11);
		encode_string(buf, &self->as_resolved);
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
