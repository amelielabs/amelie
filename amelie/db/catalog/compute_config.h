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

typedef struct ComputeConfig ComputeConfig;

struct ComputeConfig
{
	Str    name;
	Str    user;
	Str    description;
	Grants grants;
};

static inline ComputeConfig*
compute_config_allocate()
{
	ComputeConfig* self;
	self = am_malloc(sizeof(ComputeConfig));
	str_init(&self->name);
	str_init(&self->user);
	str_init(&self->description);
	grants_init(&self->grants);
	return self;
}

static inline void
compute_config_free(ComputeConfig* self)
{
	str_free(&self->name);
	str_free(&self->user);
	str_free(&self->description);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
compute_config_set_name(ComputeConfig* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
compute_config_set_user(ComputeConfig* self, Str* value)
{
	str_free(&self->user);
	str_copy(&self->user, value);
}

static inline void
compute_config_set_description(ComputeConfig* self, Str* value)
{
	str_free(&self->description);
	str_copy(&self->description, value);
}

static inline ComputeConfig*
compute_config_copy(ComputeConfig* self)
{
	auto copy = compute_config_allocate();
	compute_config_set_name(copy, &self->name);
	compute_config_set_user(copy, &self->user);
	compute_config_set_description(copy, &self->description);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline ComputeConfig*
compute_config_read(uint8_t** pos)
{
	auto self = compute_config_allocate();
	errdefer(compute_config_free, self);
	uint8_t* pos_grants = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,   "name",        &self->name        },
		{ DECODE_STR,   "user",        &self->user        },
		{ DECODE_STR,   "description", &self->description },
		{ DECODE_ARRAY, "grants",      &pos_grants        },
		{ 0,             NULL,          NULL              },
	};
	decode_obj(obj, "compute", pos);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
compute_config_write(ComputeConfig* self, Buf* buf, int flags)
{
	unused(flags);

	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, &self->name);

	// user
	encode_raw(buf, "user", 4);
	encode_str(buf, &self->user);

	// description
	encode_raw(buf, "description", 11);
	encode_str(buf, &self->description);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
