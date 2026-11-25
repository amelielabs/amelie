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
	OPT_C  = 1 << 0,
	// hidden
	OPT_H  = 1 << 1,
	// secret
	OPT_S  = 1 << 2,
	// excluded from config
	OPT_E  = 1 << 3,
	// cannot be zero
	OPT_Z  = 1 << 4,

	// can be set as uri arg
	OPT_A  = 1 << 5,
	// can be set as uri arg (endpoint)
	OPT_AE = 1 << 6
};

typedef enum
{
	OPT_BOOL,
	OPT_U32,
	OPT_U64,
	OPT_STRING,
	OPT_JSON,
	OPT_UUID
} OptType;

struct Opt
{
	Str     name;
	OptType type;
	int     flags;
	union
	{
		uint32_t* u32;
		uint64_t* u64;
		bool*     boolean;
		Str*      string;
		Uuid*     uuid;
		void*     pointer;
	};
};

static inline void
opt_init(Opt* self, const char* name, OptType type, int flags, void* value)
{
	self->type    = type;
	self->flags   = flags;
	self->pointer = value;
	str_set_cstr(&self->name, name);
}

static inline void
opt_free(Opt* self)
{
	if (self->type == OPT_STRING || self->type == OPT_JSON)
		str_free(self->string);
}

static inline bool
opt_is(Opt* self, int flag)
{
	return (self->flags & flag) > 0;
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
		unpack_bool(pos, self->boolean);
		break;
	}
	case OPT_U32:
	case OPT_U64:
	{
		if (unlikely(! data_is_int(*pos)))
			error("option '{str}': integer value expected", name);

		int64_t value;
		unpack_int(pos, &value);
		if (unlikely(value <= 0 && opt_is(self, OPT_Z)))
			error("option '{str}': cannot be set to zero", name);

		if (self->type == OPT_U32)
			*self->u32 = value;
		else
			*self->u64 = value;
		break;
	}
	case OPT_STRING:
	{
		if (unlikely(! data_is_str(*pos)))
			error("config: string expected for option '{str}'", name);
		Str value;
		unpack_str(pos, &value);
		str_assign(self->string, &value);
		break;
	}
	case OPT_JSON:
	{
		auto start = *pos;
		data_skip(pos);
		str_assign_raw(self->string, start, *pos - start);
		break;
	}
	case OPT_UUID:
	{
		if (unlikely(! data_is_str(*pos)))
			error("config: string expected for option '{str}'", name);
		Str value;
		unpack_str(pos, &value);
		if (uuid_set_nothrow(self->uuid, &value) == -1)
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
		*self->boolean = result;
		break;
	}
	case OPT_U32:
	{
		if (str_empty(value))
			error("option '{str}': value is not defined", name);
		uint32_t result = 0;
		if (str_u32(value, &result) == -1)
			error("option '{str}': integer value expected", name);
		if (unlikely(result <= 0 && opt_is(self, OPT_Z)))
			error("option '{str}': cannot be set to zero", name);
		*self->u32 = result;
		break;
	}
	case OPT_U64:
	{
		if (str_empty(value))
			error("option '{str}': value is not defined", name);
		uint64_t result = 0;
		if (str_u64(value, &result) == -1)
			error("option '{str}': integer value expected", name);
		if (unlikely(result <= 0 && opt_is(self, OPT_Z)))
			error("option '{str}': cannot be set to zero", name);
		*self->u64 = result;
		break;
	}
	case OPT_STRING:
	{
		str_assign(self->string, value);
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
		str_assign_raw(self->string, json.buf->start, buf_size(json.buf));
		break;
	}
	case OPT_UUID:
	{
		if (str_empty(value))
			error("option '{str}': value is not defined", name);
		if (uuid_set_nothrow(self->uuid, value) == -1)
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
		encode_str(buf, self->string);
		break;
	case OPT_JSON:
		if (! str_empty(self->string))
			buf_write_str(buf, self->string);
		else
			encode_null(buf);
		break;
	case OPT_BOOL:
		encode_bool(buf, *self->boolean);
		break;
	case OPT_U32:
		encode_int(buf, *self->u32);
		break;
	case OPT_U64:
		encode_int(buf, *self->u64);
		break;
	case OPT_UUID:
		encode_uuid(buf, self->uuid);
		break;
	}
}

static inline void
opt_print(Opt* self)
{
	switch (self->type) {
	case OPT_BOOL:
		info("{-24s}{s}", str_of(&self->name),
		     *self->boolean ? "true" : "false");
		break;
	case OPT_U32:
		info("{-24s}{u32}", str_of(&self->name),
		     *self->u32);
		break;
	case OPT_U64:
		info("{-24s}{u64}", str_of(&self->name),
		     *self->u64);
		break;
	case OPT_STRING:
		info("{-24s}{str}", str_of(&self->name),
		     self->string);
		break;
	case OPT_JSON:
		break;
	case OPT_UUID:
	{
		char uuid_sz[UUID_SZ];
		uuid_get(self->uuid, uuid_sz, sizeof(uuid_sz));
		info("{-24s}{s}", str_of(&self->name),
		     uuid_sz);
		break;
	}
	}
}
