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

typedef struct IndexConfig IndexConfig;

enum
{
	INDEX_UNDEF,
	INDEX_HASH,
	INDEX_TREE
};

struct IndexConfig
{
	Str     name;
	int64_t type;
	bool    unique;
	bool    primary;
	Keys    keys;
	List    link;
};

static inline IndexConfig*
index_config_allocate(Columns* columns)
{
	IndexConfig *self;
	self = am_malloc(sizeof(IndexConfig));
	self->type    = INDEX_UNDEF;
	self->unique  = false;
	self->primary = false;
	str_init(&self->name);
	keys_init(&self->keys, columns);
	list_init(&self->link);
	return self;
}

static inline void
index_config_free(IndexConfig* self)
{
	str_free(&self->name);
	keys_free(&self->keys);
	am_free(self);
}

static inline void
index_config_set_name(IndexConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
index_config_set_unique(IndexConfig* self, bool unique)
{
	self->unique = unique;
}

static inline void
index_config_set_primary(IndexConfig* self, bool primary)
{
	self->primary = primary;
}

static inline void
index_config_set_type(IndexConfig* self, int type)
{
	self->type = type;
}

static inline IndexConfig*
index_config_copy(IndexConfig* self, Columns* columns)
{
	auto copy = index_config_allocate(columns);
	index_config_set_name(copy, &self->name);
	index_config_set_type(copy, self->type);
	index_config_set_unique(copy, self->unique);
	index_config_set_primary(copy, self->primary);
	keys_copy(&copy->keys, &self->keys);
	return copy;
}

static inline IndexConfig*
index_config_read(Columns* columns, uint8_t** pos)
{
	auto self = index_config_allocate(columns);
	errdefer(index_config_free, self);
	uint8_t* keys = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "name",    &self->name    },
		{ DECODE_INT,    "type",    &self->type    },
		{ DECODE_BOOL,   "unique",  &self->unique  },
		{ DECODE_BOOL,   "primary", &self->primary },
		{ DECODE_ARRAY,  "keys",    &keys          },
		{ 0,              NULL,     NULL           },
	};
	decode_obj(obj, "index", pos);
	keys_read(&self->keys, &keys);
	return self;
}

static inline void
index_config_write(IndexConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// unique
	encode_raw(buf, "unique", 6);
	encode_bool(buf, self->unique);

	// primary
	encode_raw(buf, "primary", 7);
	encode_bool(buf, self->primary);

	// keys
	encode_raw(buf, "keys", 4);
	keys_write(&self->keys, buf);

	encode_obj_end(buf);
}
