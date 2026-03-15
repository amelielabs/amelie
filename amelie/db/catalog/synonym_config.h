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
	Str db;
	Str name;
	Str for_db;
	Str for_name;
};

static inline SynonymConfig*
synonym_config_allocate(void)
{
	SynonymConfig* self;
	self = am_malloc(sizeof(SynonymConfig));
	str_init(&self->db);
	str_init(&self->name);
	str_init(&self->for_db);
	str_init(&self->for_name);
	return self;
}

static inline void
synonym_config_free(SynonymConfig* self)
{
	str_free(&self->db);
	str_free(&self->name);
	str_free(&self->for_db);
	str_free(&self->for_name);
	am_free(self);
}

static inline void
synonym_config_set_db(SynonymConfig* self, Str* db)
{
	str_free(&self->db);
	str_copy(&self->db, db);
}

static inline void
synonym_config_set_name(SynonymConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
synonym_config_set_for_db(SynonymConfig* self, Str* name)
{
	str_free(&self->for_db);
	str_copy(&self->for_db, name);
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
	synonym_config_set_db(copy, &self->db);
	synonym_config_set_name(copy, &self->name);
	synonym_config_set_for_db(copy, &self->for_db);
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
		{ DECODE_STRING, "db",       &self->db       },
		{ DECODE_STRING, "name",     &self->name     },
		{ DECODE_STRING, "for_db"  , &self->for_db   },
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

	// db
	encode_raw(buf, "db", 2);
	encode_string(buf, &self->db);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// for_db
	encode_raw(buf, "for_db", 6);
	encode_string(buf, &self->for_db);

	// for_name
	encode_raw(buf, "for_name", 8);
	encode_string(buf, &self->for_name);

	encode_obj_end(buf);
}
