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

typedef struct Key Key;

struct Key
{
	int     order;
	Column* column;
	int64_t ref;
	int64_t type;
	Str     path;
	List    link;
};

static inline Key*
key_allocate(void)
{
	Key* self = am_malloc(sizeof(Key));
	self->order  = -1;
	self->column = NULL;
	self->ref    = -1;
	self->type   = -1;
	str_init(&self->path);
	list_init(&self->link);
	return self;
}

static inline void
key_free(Key* self)
{
	str_free(&self->path);
	am_free(self);
}

static inline void
key_set_ref(Key* self, int value)
{
	self->ref = value;
}

static inline void
key_set_type(Key* self, int value)
{
	self->type = value;
}

static inline void
key_set_path(Key* self, Str* path)
{
	str_copy(&self->path, path);
}

static inline Key*
key_copy(Key* self)
{
	auto copy = key_allocate();
	key_set_ref(copy, self->ref);
	key_set_type(copy, self->type);
	key_set_path(copy, &self->path);
	return copy;
}

static inline bool
key_compare(Key* self, Key* with)
{
	if (self->ref != with->ref)
		return false;
	return str_compare(&self->path, &with->path);
}

static inline Key*
key_read(uint8_t** pos)
{
	auto self = key_allocate();
	guard(key_free, self);
	Str type;
	str_init(&type);
	Decode obj[] =
	{
		{ DECODE_INT,         "ref",  &self->ref  },
		{ DECODE_STRING_READ, "type", &type       },
		{ DECODE_STRING,      "path", &self->path },
		{ 0,                   NULL,  NULL        },
	};
	decode_obj(obj, "key", pos);
	self->type = type_read(&type);
	if (self->type == -1)
		error("key: unknown type %.*s", str_size(&type),
		      str_of(&type));
	return unguard();
}

static inline void
key_write(Key* self, Buf* buf)
{
	encode_obj(buf);

	// ref
	encode_raw(buf, "ref", 3);
	encode_integer(buf, self->ref);

	// type
	encode_raw(buf, "type", 4);
	encode_cstr(buf, type_of(self->type));

	// path
	encode_raw(buf, "path", 4);
	encode_string(buf, &self->path);

	encode_obj_end(buf);
}
