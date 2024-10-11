#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
key_set_ref(Key* self, int ref)
{
	self->ref = ref;
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

static inline void
key_find(Key* self, uint8_t** pos)
{
	// find by path
	if (str_empty(&self->path))
		return;

	if (! obj_find_path(pos, &self->path))
		error("column %.*s: key path <%.*s> is not found",
		      str_size(&self->column->name),
		      str_of(&self->column->name),
		      str_size(&self->path),
		      str_of(&self->path));

	// validate data type
	if (! type_validate(self->type, *pos))
		error("column %.*s: key path <%.*s> does not match data type %s",
		      str_size(&self->column->name),
		      str_of(&self->column->name),
		      str_size(&self->path),
		      str_of(&self->path),
		      type_of(self->type));
}

hot static inline uint32_t
key_hash_integer(uint32_t hash, int64_t value)
{
	return hash_murmur3_32((uint8_t*)&value, sizeof(value), hash);
}

hot static inline uint32_t
key_hash_timestamp(uint32_t hash, int64_t value)
{
	return hash_murmur3_32((uint8_t*)&value, sizeof(value), hash);
}

hot static inline uint32_t
key_hash_string(uint32_t hash, Str* string)
{
	return hash_murmur3_32(str_u8(string), str_size(string), hash);
}

hot static inline uint32_t
key_hash(uint32_t hash, uint8_t* pos)
{
	if (data_is_integer(pos))
	{
		int64_t value;
		data_read_integer(&pos, &value);
		hash = key_hash_integer(hash, value);
	} else
	if (data_is_timestamp(pos))
	{
		int64_t value;
		data_read_timestamp(&pos, &value);
		hash = key_hash_timestamp(hash, value);
	} else
	if (data_is_string(pos))
	{
		Str value;
		data_read_string(&pos, &value);
		hash = key_hash_string(hash, &value);
	} else {
		abort();
	}
	return hash;
}
