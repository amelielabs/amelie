#pragma once

//
// sonata.
//
// SQL Database for JSON.
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
	bool    primary;
	Def     def;
	List    link;
};

static inline IndexConfig*
index_config_allocate(void)
{
	IndexConfig *self;
	self = so_malloc(sizeof(IndexConfig));
	self->type    = INDEX_UNDEF;
	self->primary = false;
	str_init(&self->name);
	def_init(&self->def);
	list_init(&self->link);
	return self;
}

static inline void
index_config_free(IndexConfig* self)
{
	str_free(&self->name);
	def_free(&self->def);
	so_free(self);
}

static inline void
index_config_set_name(IndexConfig* self, Str* name)
{
	str_copy(&self->name, name);
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
index_config_copy(IndexConfig* self)
{
	auto copy = index_config_allocate();
	guard(index_config_free, copy);
	index_config_set_name(copy, &self->name);
	index_config_set_type(copy, self->type);
	index_config_set_primary(copy, self->primary);
	def_copy(&copy->def, &self->def);
	return unguard();
}

static inline IndexConfig*
index_config_read(uint8_t** pos)
{
	auto self = index_config_allocate();
	guard(index_config_free, self);

	uint8_t* def = NULL;
	Decode map[] =
	{
		{ DECODE_STRING, "name",    &self->name    },
		{ DECODE_INT,    "type",    &self->type    },
		{ DECODE_BOOL,   "primary", &self->primary },
		{ DECODE_MAP,    "def",     &def           },
		{ 0,             NULL,      NULL           },
	};
	decode_map(map, pos);
	def_read(&self->def, &def);

	return unguard();
}

static inline void
index_config_write(IndexConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// primary
	encode_raw(buf, "primary", 7);
	encode_bool(buf, self->primary);

	// def
	encode_raw(buf, "def", 3);
	def_write(&self->def, buf);
}
