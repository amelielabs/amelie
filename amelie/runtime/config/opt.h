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

typedef struct Opt Opt;

enum
{
	// can be set in config or command line
	OPT_C = 1 << 0,
	// hidden
	OPT_H = 1 << 1,
	// secret
	OPT_S = 1 << 2,
	// excluded from config
	OPT_E = 1 << 3,
	// cannot be zero
	OPT_Z = 1 << 4
};

typedef enum
{
	OPT_BOOL,
	OPT_INT,
	OPT_STRING,
	OPT_JSON
} OptType;

struct Opt
{
	Str     name;
	OptType type;
	int     flags;
	List    link;
	union
	{
		atomic_u64 integer;
		Str        string;
	};
};

static inline void
opt_init(Opt* self, const char* name, OptType type, int flags)
{
	memset(self, 0, sizeof(Opt));
	self->type  = type;
	self->flags = flags;
	str_set_cstr(&self->name, name);
	list_init(&self->link);
}

static inline void
opt_free(Opt* self)
{
	if (self->type == OPT_STRING || self->type == OPT_JSON)
		str_free(&self->string);
}

static inline bool
opt_is(Opt* self, int flag)
{
	return (self->flags & flag) > 0;
}

// int
static inline void
opt_int_set(Opt* self, uint64_t value)
{
	assert(self->type == OPT_INT || self->type == OPT_BOOL);
	atomic_u64_set(&self->integer, value);
}

static inline uint64_t
opt_int_set_next(Opt* self)
{
	uint64_t value;
	value = atomic_u64_inc(&self->integer);
	return value + 1;
}

static inline void
opt_int_add(Opt* self, uint64_t value)
{
	assert(self->type == OPT_INT || self->type == OPT_BOOL);
	atomic_u64_add(&self->integer, value);
}

static inline void
opt_int_sub(Opt* self, uint64_t value)
{
	assert(self->type == OPT_INT || self->type == OPT_BOOL);
	atomic_u64_sub(&self->integer, value);
}

static inline void
opt_int_follow(Opt* self, uint64_t value)
{
	assert(self->type == OPT_INT || self->type == OPT_BOOL);
	// todo: cas loop
	if (value > atomic_u64_of(&self->integer))
		atomic_u64_set(&self->integer, value);
}

static inline uint64_t
opt_int_of(Opt* self)
{
	assert(self->type == OPT_INT || self->type == OPT_BOOL);
	return atomic_u64_of(&self->integer);
}

// string
static inline bool
opt_string_is_set(Opt* self)
{
	assert(self->type == OPT_STRING);
	return !str_empty(&self->string);
}

static inline void
opt_string_set(Opt* self, Str* str)
{
	assert(self->type == OPT_STRING);
	str_free(&self->string);
	str_copy(&self->string, str);
}

static inline void
opt_string_set_raw(Opt* self, const char* value, int size)
{
	assert(self->type == OPT_STRING);
	str_free(&self->string);
	str_dup(&self->string, value, size);
}

static inline void
opt_string_set_cstr(Opt* self, const char* value)
{
	opt_string_set_raw(self, value, strlen(value));
}

static inline Str*
opt_string_of(Opt* self)
{
	assert(self->type == OPT_STRING);
	return &self->string;
}

// data
static inline bool
opt_json_is_set(Opt* self)
{
	assert(self->type == OPT_JSON);
	return !str_empty(&self->string);
}

static inline void
opt_json_set(Opt* self, uint8_t* value, int size)
{
	assert(self->type == OPT_JSON);
	str_free(&self->string);
	str_dup(&self->string, value, size);
}

static inline void
opt_json_set_buf(Opt* self, Buf* buf)
{
	assert(self->type == OPT_JSON);
	str_free(&self->string);
	str_dup(&self->string, buf->start, buf_size(buf));
}

static inline void
opt_json_set_str(Opt* self, Str* str)
{
	assert(self->type == OPT_JSON);
	str_free(&self->string);
	str_copy(&self->string, str);
}

static inline uint8_t*
opt_json_of(Opt* self)
{
	assert(self->type == OPT_JSON);
	return str_u8(&self->string);
}

static inline void
opt_set_json(Opt* self, uint8_t** pos)
{
	auto name = &self->name;
	switch (self->type) {
	case OPT_BOOL:
	{
		if (unlikely(! json_is_bool(*pos)))
			error("option '%.*s': bool value expected",
			      str_size(name), str_of(name));
		bool value;
		json_read_bool(pos, &value);
		opt_int_set(self, value);
		break;
	}
	case OPT_INT:
	{
		if (unlikely(! json_is_integer(*pos)))
			error("option '%.*s': integer value expected",
			      str_size(name), str_of(name));
		int64_t value;
		json_read_integer(pos, &value);

		if (unlikely(value == 0 && opt_is(self, OPT_Z)))
			error("option '%.*s': cannot be set to zero",
			      str_size(name), str_of(name));
		opt_int_set(self, value);
		break;
	}
	case OPT_STRING:
	{
		if (unlikely(! json_is_string(*pos)))
			error("config: string expected for option '%.*s'",
			      str_size(name), str_of(name));
		Str value;
		json_read_string(pos, &value);
		opt_string_set(self, &value);
		break;
	}
	case OPT_JSON:
	{
		auto start = *pos;
		json_skip(pos);
		opt_json_set(self, start, *pos - start);
		break;
	}
	}
}

static inline void
opt_set(Opt* self, Str* value)
{
	// ensure value is defined
	auto name = &self->name;
	if (self->type != OPT_BOOL && str_empty(value))
		error("option '%.*s': value is not defined",
		      str_size(name), str_of(name));

	switch (self->type) {
	case OPT_BOOL:
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
		opt_int_set(self, result);
		break;
	}
	case OPT_INT:
	{
		int64_t result = 0;
		if (str_toint(value, &result) == -1)
			error("option '%.*s': integer value expected",
			      str_size(name), str_of(name));
		if (unlikely(result == 0 && opt_is(self, OPT_Z)))
			error("option '%.*s': cannot be set to zero",
			      str_size(name), str_of(name));
		opt_int_set(self, result);
		break;
	}
	case OPT_STRING:
	{
		opt_string_set(self, value);
		break;
	}
	case OPT_JSON:
	{
		Json json;
		json_init(&json);
		defer(json_free, &json);
		json_parse(&json, value, NULL);
		opt_json_set(self, json.buf->start, buf_size(json.buf));
		break;
	}
	}
}

static inline void
opt_encode(Opt* self, Buf* buf)
{
	switch (self->type) {
	case OPT_STRING:
		encode_string(buf, &self->string);
		break;
	case OPT_JSON:
		if (opt_json_is_set(self))
			buf_write_str(buf, &self->string);
		else
			encode_null(buf);
		break;
	case OPT_BOOL:
		encode_bool(buf, opt_int_of(self));
		break;
	case OPT_INT:
		encode_integer(buf, opt_int_of(self));
		break;
	}
}

static inline void
opt_print(Opt* self)
{
	switch (self->type) {
	case OPT_BOOL:
		info("%-24s%s", str_of(&self->name),
		     opt_int_of(self) ? "true" : "false");
		break;
	case OPT_INT:
		info("%-24s%" PRIu64, str_of(&self->name),
		     opt_int_of(self));
		break;
	case OPT_STRING:
		info("%-24s%.*s", str_of(&self->name),
		     str_size(&self->string), str_of(&self->string));
		break;
	case OPT_JSON:
		break;
	}
}
