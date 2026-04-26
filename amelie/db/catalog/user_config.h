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
	Str    name;
	Str    parent;
	Str    created_at;
	Str    revoked_at;
	bool   agent;
	bool   superuser;
	Grants grants;
};

static inline UserConfig*
user_config_allocate()
{
	UserConfig* self;
	self = am_malloc(sizeof(UserConfig));
	self->agent     = false;
	self->superuser = false;
	str_init(&self->name);
	str_init(&self->parent);
	str_init(&self->created_at);
	str_init(&self->revoked_at);
	grants_init(&self->grants);
	return self;
}

static inline void
user_config_free(UserConfig* self)
{
	str_free(&self->name);
	str_free(&self->parent);
	str_free(&self->created_at);
	str_free(&self->revoked_at);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
user_config_set_name(UserConfig* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
user_config_set_parent(UserConfig* self, Str* value)
{
	str_free(&self->parent);
	str_copy(&self->parent, value);
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
user_config_set_superuser(UserConfig* self, bool value)
{
	self->superuser = value;
}

static inline UserConfig*
user_config_copy(UserConfig* self)
{
	auto copy = user_config_allocate();
	user_config_set_name(copy, &self->name);
	user_config_set_parent(copy, &self->parent);
	user_config_set_created_at(copy, &self->created_at);
	user_config_set_revoked_at(copy, &self->revoked_at);
	user_config_set_agent(copy, self->agent);
	user_config_set_superuser(copy, self->superuser);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline UserConfig*
user_config_read(uint8_t** pos)
{
	auto self = user_config_allocate();
	errdefer(user_config_free, self);
	uint8_t* pos_grants = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,   "name",       &self->name       },
		{ DECODE_STR,   "parent",     &self->parent     },
		{ DECODE_STR,   "created_at", &self->created_at },
		{ DECODE_STR,   "revoked_at", &self->revoked_at },
		{ DECODE_BOOL,  "agent",      &self->agent      },
		{ DECODE_BOOL,  "superuser",  &self->superuser  },
		{ DECODE_ARRAY, "grants",     &pos_grants       },
		{ 0,             NULL,         NULL             },
	};
	decode_obj(obj, "user", pos);

	// grants
	grants_read(&self->grants, &pos_grants);
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
	encode_str(buf, &self->name);

	// parent
	encode_raw(buf, "parent", 6);
	encode_str(buf, &self->parent);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// created_at
	encode_raw(buf, "created_at", 10);
	encode_str(buf, &self->created_at);

	// revoked_at
	encode_raw(buf, "revoked_at", 10);
	encode_str(buf, &self->revoked_at);

	// agent
	encode_raw(buf, "agent", 5);
	encode_bool(buf, self->agent);

	// superuser
	encode_raw(buf, "superuser", 9);
	encode_bool(buf, self->superuser);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
