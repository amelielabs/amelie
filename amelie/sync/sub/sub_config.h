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

typedef struct SubConfig SubConfig;

struct SubConfig
{
	Str    user;
	Str    name;
	Uuid   pub_id;
	Grants grants;
};

static inline SubConfig*
sub_config_allocate(void)
{
	SubConfig* self;
	self = am_malloc(sizeof(SubConfig));
	str_init(&self->user);
	str_init(&self->name);
	uuid_init(&self->pub_id);
	grants_init(&self->grants);
	return self;
}

static inline void
sub_config_free(SubConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
sub_config_set_user(SubConfig* self, Str* name)
{
	str_free(&self->user);
	str_copy(&self->user, name);
}

static inline void
sub_config_set_name(SubConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
sub_config_set_pub_id(SubConfig* self, Uuid* value)
{
	self->pub_id = *value;
}

static inline SubConfig*
sub_config_copy(SubConfig* self)
{
	auto copy = sub_config_allocate();
	sub_config_set_user(copy, &self->user);
	sub_config_set_name(copy, &self->name);
	sub_config_set_pub_id(copy, &self->pub_id);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline SubConfig*
sub_config_read(uint8_t** pos)
{
	auto self = sub_config_allocate();
	errdefer(sub_config_free, self);
	uint8_t* pos_grants   = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "user",   &self->user   },
		{ DECODE_STRING, "name",   &self->name   },
		{ DECODE_UUID,   "pub_id", &self->pub_id },
		{ DECODE_ARRAY,  "grants", &pos_grants   },
		{ 0,              NULL,     NULL         },
	};
	decode_obj(obj, "sub", pos);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
sub_config_write(SubConfig* self, Buf* buf, int flags)
{
	// {}
	encode_obj(buf);

	// user
	encode_raw(buf, "user", 4);
	encode_string(buf, &self->user);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// pub_id
	encode_raw(buf, "pub_id", 6);
	encode_uuid(buf, &self->pub_id);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
