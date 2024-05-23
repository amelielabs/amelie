#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Key Key;

struct Key
{
	int     order;
	int64_t column;
	int64_t type;
	Str     path;
	bool    asc;
	Key*    next_column;
	Key*    next;
};

static inline Key*
key_allocate(void)
{
	Key* self = so_malloc(sizeof(Key));
	self->order       = -1;
	self->column      = -1;
	self->type        = -1;
	self->asc         = false;
	self->next_column = NULL;
	self->next        = NULL;
	str_init(&self->path);
	return self;
}

static inline void
key_free(Key* self)
{
	str_free(&self->path);
	so_free(self);
}

static inline void
key_set_column(Key* self, int column)
{
	self->column = column;
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

static inline void
key_set_asc(Key* self, bool value)
{
	self->asc = value;
}

static inline Key*
key_copy(Key* self)
{
	auto copy = key_allocate();
	guard(key_free, copy);
	key_set_column(copy, self->column);
	key_set_type(copy, self->type);
	key_set_path(copy, &self->path);
	key_set_asc(copy, self->asc);
	return unguard();
}

static inline Key*
key_read(uint8_t** pos)
{
	auto self = key_allocate();
	guard(key_free, self);
	Decode map[] =
	{
		{ DECODE_INT,    "column", &self->column },
		{ DECODE_INT,    "type",   &self->type   },
		{ DECODE_STRING, "path",   &self->path   },
		{ DECODE_BOOL,   "asc",    &self->asc    },
		{ 0,              NULL,    NULL          },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
key_write(Key* self, Buf* buf)
{
	encode_map(buf, 4);

	// column
	encode_raw(buf, "column", 6);
	encode_integer(buf, self->column);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// path
	encode_raw(buf, "path", 4);
	encode_string(buf, &self->path);

	// asc
	encode_raw(buf, "asc", 3);
	encode_bool(buf, self->asc);
}

static inline void
key_find(Column* column, Key* key, uint8_t** pos)
{
	// find by path
	if (str_empty(&key->path))
		return;

	if (! map_find_path(pos, &key->path))
		error("column %.*s: key path <%.*s> is not found",
		      str_size(&column->name),
		      str_of(&column->name),
		      str_size(&key->path),
		      str_of(&key->path));

	// validate data type
	if (! type_validate(key->type, *pos))
		error("column %.*s: key path <%.*s> does not match data type",
		      str_size(&column->name),
		      str_of(&column->name),
		      str_size(&key->path),
		      str_of(&key->path));
}
