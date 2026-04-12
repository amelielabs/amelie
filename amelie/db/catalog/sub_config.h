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
	Buf    rels;
};

static inline SubConfig*
sub_config_allocate(void)
{
	SubConfig* self;
	self = am_malloc(sizeof(SubConfig));
	str_init(&self->user);
	str_init(&self->name);
	grants_init(&self->grants);
	buf_init(&self->rels);
	return self;
}

static inline void
sub_config_free(SubConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	grants_free(&self->grants);
	buf_free(&self->rels);
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

static inline SubConfig*
sub_config_copy(SubConfig* self)
{
	auto copy = sub_config_allocate();
	sub_config_set_user(copy, &self->user);
	sub_config_set_name(copy, &self->name);
	grants_copy(&copy->grants, &self->grants);
	buf_write_buf(&copy->rels, &self->rels);
	return copy;
}

static inline SubConfig*
sub_config_read(uint8_t** pos)
{
	auto self = sub_config_allocate();
	errdefer(sub_config_free, self);
	uint8_t* pos_grants = NULL;
	uint8_t* pos_rels   = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "user",   &self->user },
		{ DECODE_STRING, "name",   &self->name },
		{ DECODE_ARRAY,  "grants", &pos_grants },
		{ DECODE_ARRAY,  "rels",   &pos_rels   },
		{ 0,              NULL,     NULL       },
	};
	decode_obj(obj, "sub", pos);

	// grants
	grants_read(&self->grants, &pos_grants);

	// rels
	json_read_array(&pos_rels);
	while (! json_read_array_end(&pos_rels))
	{
		Str str;
		json_read_string(&pos_rels, &str);
		Uuid id;
		uuid_set(&id, &str);
		buf_write(&self->rels, &id, sizeof(id));
	}
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

	// rels
	encode_raw(buf, "rels", 4);
	encode_array(buf);
	auto id  = (Uuid*)self->rels.start;
	auto end = (Uuid*)self->rels.position;
	for (; id < end; id++)
		encode_uuid(buf, id);
	encode_array_end(buf);

	encode_obj_end(buf);
}
