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
	Str     user;
	Str     name;
	Uuid    id;
	Str     on_user;
	Str     on;
	int64_t lsn;
	int64_t op;
	Grants  grants;
};

static inline SubConfig*
sub_config_allocate(void)
{
	SubConfig* self;
	self = am_malloc(sizeof(SubConfig));
	self->lsn = 0;
	self->op  = 0;
	str_init(&self->user);
	str_init(&self->name);
	str_init(&self->on_user);
	str_init(&self->on);
	uuid_init(&self->id);
	grants_init(&self->grants);
	return self;
}

static inline void
sub_config_free(SubConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	str_free(&self->on_user);
	str_free(&self->on);
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
sub_config_set_on_user(SubConfig* self, Str* name)
{
	str_free(&self->on_user);
	str_copy(&self->on_user, name);
}

static inline void
sub_config_set_on(SubConfig* self, Str* name)
{
	str_free(&self->on);
	str_copy(&self->on, name);
}

static inline void
sub_config_set_pos(SubConfig* self, int64_t lsn, int64_t op)
{
	self->lsn = lsn;
	self->op  = op;
}

static inline SubConfig*
sub_config_copy(SubConfig* self)
{
	auto copy = sub_config_allocate();
	sub_config_set_user(copy, &self->user);
	sub_config_set_name(copy, &self->name);
	sub_config_set_id(copy, &self->id);
	sub_config_set_on_user(copy, &self->on_user);
	sub_config_set_on(copy, &self->on);
	sub_config_set_pos(copy, self->lsn, self->op);
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
		{ DECODE_STR,   "user",    &self->user    },
		{ DECODE_STR,   "name",    &self->name    },
		{ DECODE_UUID,  "id",      &self->id      },
		{ DECODE_STR,   "on_user", &self->on_user },
		{ DECODE_STR,   "on",      &self->on      },
		{ DECODE_INT,   "lsn",     &self->lsn     },
		{ DECODE_INT,   "op",      &self->op      },
		{ DECODE_ARRAY, "grants",  &pos_grants    },
		{ 0,             NULL,      NULL          },
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
	encode_str(buf, &self->user);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, &self->name);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// on_user
	encode_raw(buf, "on_user", 7);
	encode_str(buf, &self->on_user);

	// on
	encode_raw(buf, "on", 2);
	encode_str(buf, &self->on);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_int(buf, self->lsn);

	// op
	encode_raw(buf, "op", 2);
	encode_int(buf, self->op);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
