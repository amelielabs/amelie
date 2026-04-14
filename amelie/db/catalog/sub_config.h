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
	Grants grants;
	Uuid   id;
	Uuid   id_rel;
};

static inline SubConfig*
sub_config_allocate(void)
{
	SubConfig* self;
	self = am_malloc(sizeof(SubConfig));
	str_init(&self->user);
	str_init(&self->name);
	grants_init(&self->grants);
	uuid_init(&self->id_rel);
	uuid_init(&self->id_rel);
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
sub_config_set_id(SubConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
sub_config_set_id_rel(SubConfig* self, Uuid* id)
{
	self->id_rel = *id;
}

static inline SubConfig*
sub_config_copy(SubConfig* self)
{
	auto copy = sub_config_allocate();
	sub_config_set_user(copy, &self->user);
	sub_config_set_name(copy, &self->name);
	sub_config_set_id(copy, &self->id);
	sub_config_set_id_rel(copy, &self->id_rel);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline SubConfig*
sub_config_read(uint8_t** pos)
{
	auto self = sub_config_allocate();
	errdefer(sub_config_free, self);
	uint8_t* pos_grants = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "user",   &self->user   },
		{ DECODE_STRING, "name",   &self->name   },
		{ DECODE_ARRAY,  "grants", &pos_grants   },
		{ DECODE_UUID,   "id",     &self->id     },
		{ DECODE_UUID,   "id_rel", &self->id_rel },
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

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// id_rel
	encode_raw(buf, "id_rel", 6);
	encode_uuid(buf, &self->id_rel);

	encode_obj_end(buf);
}
