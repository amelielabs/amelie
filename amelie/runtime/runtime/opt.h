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
	// can be set as uri arg
	OPT_U = 1 << 1,
	// hidden
	OPT_H = 1 << 2,
	// secret
	OPT_S = 1 << 3,
	// excluded from config
	OPT_E = 1 << 4,
	// cannot be zero
	OPT_Z = 1 << 5
};

typedef enum
{
	OPT_BOOL,
	OPT_INT,
	OPT_STRING,
	OPT_JSON,
	OPT_UUID
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
		Uuid       uuid;
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
opt_string_empty(Opt* self)
{
	assert(self->type == OPT_STRING);
	return str_empty(&self->string);
}

static inline void
opt_string_set(Opt* self, Str* str)
{
	assert(self->type == OPT_STRING);
	str_free(&self->string);
	if (! str_empty(str))
		str_copy(&self->string, str);
}

static inline void
opt_string_set_raw(Opt* self, const char* value, int size)
{
	assert(self->type == OPT_STRING);
	str_free(&self->string);
	if (size > 0)
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
opt_json_empty(Opt* self)
{
	assert(self->type == OPT_JSON);
	return str_empty(&self->string);
}

static inline void
opt_json_set(Opt* self, uint8_t* value, int size)
{
	assert(self->type == OPT_JSON);
	str_free(&self->string);
	str_dup(&self->string, value, size);
}

static inline void
opt_json_set_data(Opt* self, uint8_t* value)
{
	assert(self->type == OPT_JSON);
	str_free(&self->string);
	str_set(&self->string, (char*)value, data_sizeof(value));
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

// uuid
static inline bool
opt_uuid_empty(Opt* self)
{
	assert(self->type == OPT_UUID);
	return uuid_empty(&self->uuid);
}

static inline void
opt_uuid_set(Opt* self, Uuid* id)
{
	assert(self->type == OPT_UUID);
	self->uuid = *id;
}

static inline Uuid*
opt_uuid_of(Opt* self)
{
	assert(self->type == OPT_UUID);
	return &self->uuid;
}

static inline void
opt_set_json(Opt* self, uint8_t** pos)
{
	auto name = &self->name;
	switch (self->type) {
	case OPT_BOOL:
	{
		if (unlikely(! data_is_bool(*pos)))
			error("option '{str}': bool value expected", name);
		bool value;
		unpack_bool(pos, &value);
		opt_int_set(self, value);
		break;
	}
	case OPT_INT:
	{
		if (unlikely(! data_is_int(*pos)))
			error("option '{str}': integer value expected", name);
		int64_t value;
		unpack_int(pos, &value);

		if (unlikely(value == 0 && opt_is(self, OPT_Z)))
			error("option '{str}': cannot be set to zero", name);
		opt_int_set(self, value);
		break;
	}
	case OPT_STRING:
	{
		if (unlikely(! data_is_str(*pos)))
			error("config: string expected for option '{str}'", name);
		Str value;
		unpack_str(pos, &value);
		opt_string_set(self, &value);
		break;
	}
	case OPT_JSON:
	{
		auto start = *pos;
		data_skip(pos);
		opt_json_set(self, start, *pos - start);
		break;
	}
	case OPT_UUID:
	{
		if (unlikely(! data_is_str(*pos)))
			error("config: string expected for option '{str}'", name);
		Str value;
		unpack_str(pos, &value);
		if (uuid_set_nothrow(&self->uuid, &value) == -1)
			error("option '{str}': failed to parse uuid", name);
		break;
	}
	}
}

static inline void
opt_set(Opt* self, Str* value)
{
	// ensure value is defined
	auto name = &self->name;
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
				error("option '{str}': bool value expected", name);
		}
		opt_int_set(self, result);
		break;
	}
	case OPT_INT:
	{
		if (str_empty(value))
			error("option '{str}': value is not defined", name);
		int64_t result = 0;
		if (str_toint(value, &result) == -1)
			error("option '{str}': integer value expected", name);
		if (unlikely(result == 0 && opt_is(self, OPT_Z)))
			error("option '{str}': cannot be set to zero", name);
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
		if (str_empty(value))
			error("option '{str}': value is not defined", name);
		Json json;
		json_init(&json);
		defer(json_free, &json);
		json_parse(&json, value, NULL);
		opt_json_set(self, json.buf->start, buf_size(json.buf));
		break;
	}
	case OPT_UUID:
	{
		if (str_empty(value))
			error("option '{str}': value is not defined", name);
		if (uuid_set_nothrow(&self->uuid, value) == -1)
			error("option '{str}': failed to parse uuid", name);
		break;
	}
	}
}

static inline void
opt_encode(Opt* self, Buf* buf)
{
	switch (self->type) {
	case OPT_STRING:
		encode_str(buf, &self->string);
		break;
	case OPT_JSON:
		if (! opt_json_empty(self))
			buf_write_str(buf, &self->string);
		else
			encode_null(buf);
		break;
	case OPT_BOOL:
		encode_bool(buf, opt_int_of(self));
		break;
	case OPT_INT:
		encode_int(buf, opt_int_of(self));
		break;
	case OPT_UUID:
		encode_uuid(buf, opt_uuid_of(self));
		break;
	}
}

static inline void
opt_print(Opt* self)
{
	switch (self->type) {
	case OPT_BOOL:
		info("{-24s}{s}", str_of(&self->name),
		     opt_int_of(self) ? "true" : "false");
		break;
	case OPT_INT:
		info("{-24s}{u64}", str_of(&self->name),
		     opt_int_of(self));
		break;
	case OPT_STRING:
		info("{-24s}{str}", str_of(&self->name),
		     &self->string);
		break;
	case OPT_JSON:
		break;
	case OPT_UUID:
	{
		char uuid_sz[UUID_SZ];
		uuid_get(&self->uuid, uuid_sz, sizeof(uuid_sz));
		info("{-24s}{s}", str_of(&self->name),
		     uuid_sz);
		break;
	}
	}
}
