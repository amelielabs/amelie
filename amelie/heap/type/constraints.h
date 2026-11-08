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
	bool    identity;
	int64_t identity_modulo;
	int64_t decimal;
	int64_t decimal_scale;
	Buf     value;
};

static inline void
constraints_init(Constraints* self)
{
	self->not_null        = false;
	self->identity        = false;
	self->identity_modulo = INT64_MAX;
	self->decimal         = 0;
	self->decimal_scale   = 0;
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
constraints_set_identity(Constraints* self, bool value)
{
	self->identity = value;
}

static inline void
constraints_set_identity_modulo(Constraints* self, int64_t value)
{
	self->identity_modulo = value;
}

static inline void
constraints_set_decimal(Constraints* self, int precision, int scale)
{
	self->decimal = precision;
	self->decimal_scale = scale;
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
	constraints_set_identity(copy, self->identity);
	constraints_set_identity_modulo(copy, self->identity_modulo);
	constraints_set_decimal(copy, self->decimal, self->decimal_scale);
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
		if (str_is_case(&name, "identity", 8))
			unpack_bool(pos, &self->identity);
		else
		if (str_is_case(&name, "identity_modulo", 15))
			unpack_int(pos, &self->identity_modulo);
		else
		if (str_is_case(&name, "decimal", 7))
		{
			unpack_int(pos, &self->decimal);
			unpack_int(pos, &self->decimal_scale);
		} else
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
	unused(flags);

	// not_null
	if (self->not_null)
	{
		encode_array(buf);
		encode_raw(buf, "not_null", 8);
		encode_bool(buf, self->not_null);
		encode_array_end(buf);
	}

	// identity
	if (self->identity)
	{
		encode_array(buf);
		encode_raw(buf, "identity", 8);
		encode_bool(buf, self->identity);
		encode_array_end(buf);
	}

	// identity_modulo
	if (self->identity_modulo != INT64_MAX)
	{
		encode_array(buf);
		encode_raw(buf, "identity_modulo", 15);
		encode_int(buf, self->identity_modulo);
		encode_array_end(buf);
	}

	// decimal
	if (self->decimal)
	{
		encode_array(buf);
		encode_raw(buf, "decimal", 7);
		encode_int(buf, self->decimal);
		encode_int(buf, self->decimal_scale);
		encode_array_end(buf);
	}

	// default
	if (! buf_empty(&self->value))
	{
		encode_array(buf);
		encode_raw(buf, "default", 7);
		encode_base64(buf, &self->value);
		encode_array_end(buf);
	}

	encode_array_end(buf);
}
