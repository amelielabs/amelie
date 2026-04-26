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

typedef struct UdfConfig UdfConfig;

struct UdfConfig
{
	Str     user;
	Str     name;
	Str     text;
	Columns args;
	int64_t type;
	Columns returning;
	Grants  grants;
};

static inline UdfConfig*
udf_config_allocate(void)
{
	UdfConfig* self;
	self = am_malloc(sizeof(UdfConfig));
	self->type = TYPE_NULL;
	str_init(&self->user);
	str_init(&self->name);
	str_init(&self->text);
	columns_init(&self->args);
	columns_init(&self->returning);
	grants_init(&self->grants);
	return self;
}

static inline void
udf_config_free(UdfConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	str_free(&self->text);
	columns_free(&self->args);
	columns_free(&self->returning);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
udf_config_set_user(UdfConfig* self, Str* user)
{
	str_free(&self->user);
	str_copy(&self->user, user);
}

static inline void
udf_config_set_name(UdfConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
udf_config_set_text(UdfConfig* self, Str* text)
{
	str_copy(&self->text, text);
}

static inline void
udf_config_set_type(UdfConfig* self, Type type)
{
	self->type = type;
}

static inline UdfConfig*
udf_config_copy(UdfConfig* self)
{
	auto copy = udf_config_allocate();
	udf_config_set_user(copy, &self->user);
	udf_config_set_name(copy, &self->name);
	udf_config_set_text(copy, &self->text);
	udf_config_set_type(copy, self->type);
	columns_copy(&copy->args, &self->args);
	columns_copy(&copy->returning, &self->returning);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline UdfConfig*
udf_config_read(uint8_t** pos)
{
	auto self = udf_config_allocate();
	errdefer(udf_config_free, self);
	uint8_t* pos_args      = NULL;
	uint8_t* pos_returning = NULL;
	uint8_t* pos_grants    = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,   "user",      &self->user    },
		{ DECODE_STR,   "name",      &self->name    },
		{ DECODE_STR,   "text",      &self->text    },
		{ DECODE_ARRAY, "args",      &pos_args      },
		{ DECODE_INT,   "type",      &self->type    },
		{ DECODE_ARRAY, "returning", &pos_returning },
		{ DECODE_ARRAY, "grants",    &pos_grants    },
		{ 0,             NULL,        NULL          },
	};
	decode_obj(obj, "udf", pos);
	columns_read(&self->args, &pos_args);
	columns_read(&self->returning, &pos_returning);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
udf_config_write(UdfConfig* self, Buf* buf, int flags)
{
	// map
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

	// text
	encode_raw(buf, "text", 4);
	encode_str(buf, &self->text);

	// type
	encode_raw(buf, "type", 4);
	encode_int(buf, self->type);

	// args
	encode_raw(buf, "args", 4);
	columns_write(&self->args, buf, flags);

	// returning
	encode_raw(buf, "returning", 9);
	columns_write(&self->returning, buf, flags);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
