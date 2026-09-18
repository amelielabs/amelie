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

typedef struct Api Api;

struct Api
{
	Str  description;
	Str  uri;
	Str  rel_user;
	Str  rel;
	List link;
};

static inline Api*
api_allocate()
{
	Api* self;
	self = am_malloc(sizeof(Api));

	str_init(&self->description);
	str_init(&self->uri);
	str_init(&self->rel_user);
	str_init(&self->rel);
	list_init(&self->link);
	return self;
}

static inline void
api_free(Api* self)
{
	str_free(&self->description);
	str_free(&self->uri);
	str_free(&self->rel_user);
	str_free(&self->rel);
	am_free(self);
}

static inline void
api_set_description(Api* self, Str* value)
{
	str_free(&self->description);
	str_copy(&self->description, value);
}

static inline void
api_set_uri(Api* self, Str* value)
{
	str_free(&self->uri);
	str_copy(&self->uri, value);
}

static inline void
api_set_rel_user(Api* self, Str* value)
{
	str_free(&self->rel_user);
	str_copy(&self->rel_user, value);
}

static inline void
api_set_rel(Api* self, Str* value)
{
	str_free(&self->rel);
	str_copy(&self->rel, value);
}

static inline Api*
api_copy(Api* self)
{
	auto copy = api_allocate();
	api_set_description(copy, &self->description);
	api_set_uri(copy, &self->uri);
	api_set_rel_user(copy, &self->rel_user);
	api_set_rel(copy, &self->rel);
	return copy;
}

static inline Api*
api_read(uint8_t** pos)
{
	auto self = api_allocate();
	errdefer(api_free, self);
	Decode obj[] =
	{
		{ DECODE_STR, "description", &self->description },
		{ DECODE_STR, "uri",         &self->uri         },
		{ DECODE_STR, "rel_user",    &self->rel_user    },
		{ DECODE_STR, "rel",         &self->rel         },
		{ 0,           NULL,          NULL              },
	};
	decode_obj(obj, "api", pos);
	return self;
}

static inline void
api_write(Api* self, Buf* buf, int flags)
{
	unused(flags);

	// obj
	encode_obj(buf);

	// description
	encode_raw(buf, "description", 11);
	encode_str(buf, &self->description);

	// uri
	encode_raw(buf, "uri", 3);
	encode_str(buf, &self->uri);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// rel_user
	encode_raw(buf, "rel_user", 8);
	encode_str(buf, &self->rel_user);

	// rel
	encode_raw(buf, "rel", 3);
	encode_str(buf, &self->rel);

	encode_obj_end(buf);
}
