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

typedef struct SynonymConfig SynonymConfig;

struct SynonymConfig
{
	Str user;
	Str name;
	Str for_user;
	Str for_name;
};

static inline SynonymConfig*
synonym_config_allocate(void)
{
	SynonymConfig* self;
	self = am_malloc(sizeof(SynonymConfig));
	str_init(&self->user);
	str_init(&self->name);
	str_init(&self->for_user);
	str_init(&self->for_name);
	return self;
}

static inline void
synonym_config_free(SynonymConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	str_free(&self->for_user);
	str_free(&self->for_name);
	am_free(self);
}

static inline void
synonym_config_set_user(SynonymConfig* self, Str* user)
{
	str_free(&self->user);
	str_copy(&self->user, user);
}

static inline void
synonym_config_set_name(SynonymConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
synonym_config_set_for_user(SynonymConfig* self, Str* name)
{
	str_free(&self->for_user);
	str_copy(&self->for_user, name);
}

static inline void
synonym_config_set_for_name(SynonymConfig* self, Str* name)
{
	str_free(&self->for_name);
	str_copy(&self->for_name, name);
}

static inline SynonymConfig*
synonym_config_copy(SynonymConfig* self)
{
	auto copy = synonym_config_allocate();
	synonym_config_set_user(copy, &self->user);
	synonym_config_set_name(copy, &self->name);
	synonym_config_set_for_user(copy, &self->for_user);
	synonym_config_set_for_name(copy, &self->for_name);
	return copy;
}

static inline SynonymConfig*
synonym_config_read(uint8_t** pos)
{
	auto self = synonym_config_allocate();
	errdefer(synonym_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "user",     &self->user     },
		{ DECODE_STRING, "name",     &self->name     },
		{ DECODE_STRING, "for_user", &self->for_user },
		{ DECODE_STRING, "for_name", &self->for_name },
		{ 0,              NULL,       NULL           },
	};
	decode_obj(obj, "synonym", pos);
	return self;
}

static inline void
synonym_config_write(SynonymConfig* self, Buf* buf, int flags)
{
	unused(flags);

	// map
	encode_obj(buf);

	// user
	encode_raw(buf, "user", 4);
	encode_string(buf, &self->user);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// for_user
	encode_raw(buf, "for_user", 8);
	encode_string(buf, &self->for_user);

	// for_name
	encode_raw(buf, "for_name", 8);
	encode_string(buf, &self->for_name);

	encode_obj_end(buf);
}
