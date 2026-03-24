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

typedef struct UserConfig UserConfig;

struct UserConfig
{
	Str  name;
	bool agent;
	bool system;
};

static inline UserConfig*
user_config_allocate()
{
	UserConfig* self;
	self = am_malloc(sizeof(UserConfig));
	self->agent  = false;
	self->system = false;
	str_init(&self->name);
	return self;
}

static inline void
user_config_free(UserConfig* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
user_config_set_system(UserConfig* self, bool system)
{
	self->system = system;
}

static inline void
user_config_set_name(UserConfig* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
user_config_set_agent(UserConfig* self, bool value)
{
	self->agent = value;
}

static inline UserConfig*
user_config_copy(UserConfig* self)
{
	auto copy = user_config_allocate();
	user_config_set_name(copy, &self->name);
	user_config_set_agent(copy, self->agent);
	user_config_set_system(copy, self->system);
	return copy;
}

static inline UserConfig*
user_config_read(uint8_t** pos)
{
	auto self = user_config_allocate();
	errdefer(user_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name",   &self->name   },
		{ DECODE_BOOL,   "agent",  &self->agent  },
		{ 0,              NULL,     NULL         },
	};
	decode_obj(obj, "user", pos);
	return self;
}

static inline void
user_config_write(UserConfig* self, Buf* buf, int flags)
{
	unused(flags);

	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// agent
	encode_raw(buf, "agent", 5);
	encode_bool(buf, self->agent);

	encode_obj_end(buf);
}
