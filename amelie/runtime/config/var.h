#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Var    Var;
typedef struct VarDef VarDef;

enum
{
	// can be set in config or start option
	VAR_C = 1 << 0,
	// can be set in runtime
	VAR_R = 1 << 1,
	// hidden
	VAR_H = 1 << 2,
	// secret
	VAR_S = 1 << 3,
	// system
	VAR_Y = 1 << 4,
	// excluded from config
	VAR_E = 1 << 5,
	// cannot be zero
	VAR_Z = 1 << 6,
	// local
	VAR_L = 1 << 7
};

typedef enum
{
	VAR_BOOL,
	VAR_INT,
	VAR_STRING,
	VAR_DATA
} VarType;

struct Var
{
	Str     name;
	VarType type;
	int     flags;
	List    link;
	union
	{
		atomic_u64 integer;
		Str        string;
	};
};

struct VarDef
{
	const char* name;
	VarType     type;
	int         flags;
	Var*        var;
	char*       default_string;
	uint64_t    default_int;
};

static inline void
var_init(Var* self, const char* name, VarType type, int flags)
{
	memset(self, 0, sizeof(Var));
	self->type  = type;
	self->flags = flags;
	str_set_cstr(&self->name, name);
	list_init(&self->link);
}

static inline void
var_free(Var* self)
{
	if (self->type == VAR_STRING || self->type == VAR_DATA)
		str_free(&self->string);
}

static inline bool
var_is(Var* self, int flag)
{
	return (self->flags & flag) > 0;
}

// int
static inline void
var_int_set(Var* self, uint64_t value)
{
	assert(self->type == VAR_INT || self->type == VAR_BOOL);
	atomic_u64_set(&self->integer, value);
}

static inline uint64_t
var_int_set_next(Var* self)
{
	uint64_t value;
	value = atomic_u64_inc(&self->integer);
	return value + 1;
}

static inline void
var_int_follow(Var* self, uint64_t value)
{
	assert(self->type == VAR_INT || self->type == VAR_BOOL);
	// todo: cas loop
	if (value > atomic_u64_of(&self->integer))
		atomic_u64_set(&self->integer, value);
}

static inline uint64_t
var_int_of(Var* self)
{
	assert(self->type == VAR_INT || self->type == VAR_BOOL);
	return atomic_u64_of(&self->integer);
}

// string
static inline bool
var_string_is_set(Var* self)
{
	assert(self->type == VAR_STRING);
	return !str_empty(&self->string);
}

static inline void
var_string_set(Var* self, Str* str)
{
	assert(self->type == VAR_STRING);
	str_free(&self->string);
	str_copy(&self->string, str);
}

static inline void
var_string_set_raw(Var* self, const char* value, int size)
{
	assert(self->type == VAR_STRING);
	str_free(&self->string);
	str_dup(&self->string, value, size);
}

static inline Str*
var_string_of(Var* self)
{
	assert(self->type == VAR_STRING);
	return &self->string;
}

// data
static inline bool
var_data_is_set(Var* self)
{
	assert(self->type == VAR_DATA);
	return !str_empty(&self->string);
}

static inline void
var_data_set(Var* self, uint8_t* value, int size)
{
	assert(self->type == VAR_DATA);
	str_free(&self->string);
	str_dup(&self->string, value, size);
}

static inline void
var_data_set_buf(Var* self, Buf* buf)
{
	assert(self->type == VAR_DATA);
	str_free(&self->string);
	str_dup(&self->string, buf->start, buf_size(buf));
}

static inline void
var_data_set_str(Var* self, Str* str)
{
	assert(self->type == VAR_DATA);
	str_free(&self->string);
	str_copy(&self->string, str);
}

static inline uint8_t*
var_data_of(Var* self)
{
	assert(self->type == VAR_DATA);
	return str_u8(&self->string);
}

static inline void
var_set_data(Var* self, uint8_t** pos)
{
	auto name = &self->name;
	switch (self->type) {
	case VAR_BOOL:
	{
		if (unlikely(! data_is_bool(*pos)))
			error("option '%.*s': bool value expected",
			      str_size(name), str_of(name));
		bool value;
		data_read_bool(pos, &value);
		var_int_set(self, value);
		break;
	}
	case VAR_INT:
	{
		if (unlikely(! data_is_integer(*pos)))
			error("option '%.*s': integer value expected",
			      str_size(name), str_of(name));
		int64_t value;
		data_read_integer(pos, &value);

		if (unlikely(value == 0 && var_is(self, VAR_Z)))
			error("option '%.*s': cannot be set to zero",
			      str_size(name), str_of(name));
		var_int_set(self, value);
		break;
	}
	case VAR_STRING:
	{
		if (unlikely(! data_is_string(*pos)))
			error("config: string expected for option '%.*s'",
			      str_size(name), str_of(name));
		Str value;
		data_read_string(pos, &value);
		var_string_set(self, &value);
		break;
	}
	case VAR_DATA:
	{
		auto start = *pos;
		data_skip(pos);
		var_data_set(self, start, *pos - start);
		break;
	}
	}
}

static inline void
var_set(Var* self, Str* value)
{
	// ensure value is defined
	auto name = &self->name;
	if (self->type != VAR_BOOL && str_empty(value))
		error("option '%.*s': value is not defined",
		      str_size(name), str_of(name));

	switch (self->type) {
	case VAR_BOOL:
	{
		bool result = true;
		if (! str_empty(value))
		{
			if (str_is_cstr(value, "true"))
				result = true;
			else
			if (str_is_cstr(value, "false"))
				result = false;
			else
				error("option '%.*s': bool value expected",
				      str_size(name), str_of(name));
		}
		var_int_set(self, result);
		break;
	}
	case VAR_INT:
	{
		int64_t result = 0;
		if (str_toint(value, &result) == -1)
			error("option '%.*s': integer value expected",
			      str_size(name), str_of(name));
		if (unlikely(result == 0 && var_is(self, VAR_Z)))
			error("option '%.*s': cannot be set to zero",
			      str_size(name), str_of(name));
		var_int_set(self, result);
		break;
	}
	case VAR_STRING:
	{
		var_string_set(self, value);
		break;
	}
	case VAR_DATA:
	{
		Json json;
		json_init(&json);
		guard(json_free, &json);
		json_parse(&json, value, NULL);
		var_data_set(self, json.buf->start, buf_size(json.buf));
		break;
	}
	}
}

static inline void
var_encode(Var* self, Buf* buf)
{
	switch (self->type) {
	case VAR_STRING:
		encode_string(buf, &self->string);
		break;
	case VAR_DATA:
		if (var_data_is_set(self))
			buf_write_str(buf, &self->string);
		else
			encode_null(buf);
		break;
	case VAR_BOOL:
		encode_bool(buf, var_int_of(self));
		break;
	case VAR_INT:
		encode_integer(buf, var_int_of(self));
		break;
	}
}

static inline void
var_print(Var* self)
{
	switch (self->type) {
	case VAR_BOOL:
		info("%-32s%s", str_of(&self->name),
		     var_int_of(self) ? "true" : "false");
		break;
	case VAR_INT:
		info("%-32s%" PRIu64, str_of(&self->name),
		     var_int_of(self));
		break;
	case VAR_STRING:
		info("%-32s%.*s", str_of(&self->name),
		     str_size(&self->string), str_of(&self->string));
		break;
	case VAR_DATA:
		break;
	}
}
