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

typedef struct ChannelConfig ChannelConfig;

struct ChannelConfig
{
	Str    user;
	Str    name;
	Uuid   id;
	Grants grants;
};

static inline ChannelConfig*
channel_config_allocate(void)
{
	ChannelConfig* self;
	self = am_malloc(sizeof(ChannelConfig));
	str_init(&self->user);
	str_init(&self->name);
	uuid_init(&self->id);
	grants_init(&self->grants);
	return self;
}

static inline void
channel_config_free(ChannelConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
channel_config_set_user(ChannelConfig* self, Str* name)
{
	str_free(&self->user);
	str_copy(&self->user, name);
}

static inline void
channel_config_set_name(ChannelConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
channel_config_set_id(ChannelConfig* self, Uuid* value)
{
	self->id = *value;
}

static inline ChannelConfig*
channel_config_copy(ChannelConfig* self)
{
	auto copy = channel_config_allocate();
	channel_config_set_user(copy, &self->user);
	channel_config_set_name(copy, &self->name);
	channel_config_set_id(copy, &self->id);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline ChannelConfig*
channel_config_read(uint8_t** pos)
{
	auto self = channel_config_allocate();
	errdefer(channel_config_free, self);
	uint8_t* pos_grants   = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "user",   &self->user },
		{ DECODE_STRING, "name",   &self->name },
		{ DECODE_UUID,   "id",     &self->id   },
		{ DECODE_ARRAY,  "grants", &pos_grants },
		{ 0,              NULL,     NULL       },
	};
	decode_obj(obj, "channel", pos);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
channel_config_write(ChannelConfig* self, Buf* buf, int flags)
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

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
