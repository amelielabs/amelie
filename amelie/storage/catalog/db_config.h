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

typedef struct DbConfig DbConfig;

struct DbConfig
{
	Str    name;
	Str    description;
	Uuid   id;
	Str    created_at;
	Str    revoked_at;
	bool   system;
	Limits limits;
};

static inline DbConfig*
db_config_allocate()
{
	DbConfig* self;
	self = am_malloc(sizeof(DbConfig));
	self->system = false;
	str_init(&self->name);
	str_init(&self->description);
	uuid_init(&self->id);
	str_init(&self->created_at);
	str_init(&self->revoked_at);
	limits_init(&self->limits);
	return self;
}

static inline void
db_config_free(DbConfig* self)
{
	str_free(&self->name);
	str_free(&self->description);
	str_free(&self->created_at);
	str_free(&self->revoked_at);
	am_free(self);
}

static inline void
db_config_set_name(DbConfig* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
db_config_set_description(DbConfig* self, Str* value)
{
	str_free(&self->description);
	str_copy(&self->description, value);
}

static inline void
db_config_set_created_at(DbConfig* self, Str* value)
{
	str_free(&self->created_at);
	str_copy(&self->created_at, value);
}

static inline void
db_config_set_revoked_at(DbConfig* self, Str* value)
{
	str_free(&self->revoked_at);
	str_copy(&self->revoked_at, value);
}

static inline void
db_config_set_system(DbConfig* self, bool value)
{
	self->system = value;
}

static inline DbConfig*
db_config_copy(DbConfig* self)
{
	auto copy = db_config_allocate();
	db_config_set_name(copy, &self->name);
	db_config_set_description(copy, &self->description);
	db_config_set_created_at(copy, &self->created_at);
	db_config_set_revoked_at(copy, &self->revoked_at);
	db_config_set_system(copy, self->system);
	limits_copy(&copy->limits, &self->limits);
	return copy;
}

static inline DbConfig*
db_config_read(uint8_t** pos)
{
	auto self = db_config_allocate();
	errdefer(db_config_free, self);
	uint8_t* pos_limits = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,  "name",        &self->name        },
		{ DECODE_STR,  "description", &self->description },
		{ DECODE_STR,  "created_at",  &self->created_at  },
		{ DECODE_STR,  "revoked_at",  &self->revoked_at  },
		{ DECODE_BOOL, "system",      &self->system      },
		{ DECODE_OBJ,  "limits",      &pos_limits        },
		{ 0,            NULL,          NULL              },
	};
	decode_obj(obj, "db", pos);

	// limits
	limits_read(&self->limits, &pos_limits);
	return self;
}

static inline void
db_config_write(DbConfig* self, Buf* buf, int flags)
{
	unused(flags);

	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, &self->name);

	// description
	encode_raw(buf, "description", 11);
	encode_str(buf, &self->description);

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

	// system
	encode_raw(buf, "system", 6);
	encode_bool(buf, self->system);

	// limits
	encode_raw(buf, "limits", 6);
	limits_write(&self->limits, buf);

	encode_obj_end(buf);
}
