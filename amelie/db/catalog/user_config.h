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
	Str  created_at;
	Str  revoked_at;
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
	str_init(&self->created_at);
	str_init(&self->revoked_at);
	str_init(&self->name);
	return self;
}

static inline void
user_config_free(UserConfig* self)
{
	str_free(&self->name);
	str_free(&self->created_at);
	str_free(&self->revoked_at);
	am_free(self);
}

static inline void
user_config_set_name(UserConfig* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
user_config_set_created_at(UserConfig* self, Str* value)
{
	str_free(&self->created_at);
	str_copy(&self->created_at, value);
}

static inline void
user_config_set_revoked_at(UserConfig* self, Str* value)
{
	str_free(&self->revoked_at);
	str_copy(&self->revoked_at, value);
}

static inline void
user_config_set_agent(UserConfig* self, bool value)
{
	self->agent = value;
}

static inline void
user_config_set_system(UserConfig* self, bool system)
{
	self->system = system;
}

static inline UserConfig*
user_config_copy(UserConfig* self)
{
	auto copy = user_config_allocate();
	user_config_set_name(copy, &self->name);
	user_config_set_created_at(copy, &self->created_at);
	user_config_set_revoked_at(copy, &self->revoked_at);
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
		{ DECODE_STRING, "name",       &self->name       },
		{ DECODE_STRING, "created_at", &self->created_at },
		{ DECODE_STRING, "revoked_at", &self->revoked_at },
		{ DECODE_BOOL,   "agent",      &self->agent      },
		{ 0,              NULL,         NULL             },
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

	// created_at
	encode_raw(buf, "created_at", 10);
	encode_string(buf, &self->created_at);

	// revoked_at
	encode_raw(buf, "revoked_at", 10);
	encode_string(buf, &self->revoked_at);

	// agent
	encode_raw(buf, "agent", 5);
	encode_bool(buf, self->agent);

	encode_obj_end(buf);
}
