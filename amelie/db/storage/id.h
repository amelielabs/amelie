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

typedef struct Id Id;

struct Id
{
	Uuid     id_table;
	uint32_t id_part;
	uint32_t id_page;
	uint32_t id_column;
} packed;

static inline void
id_init(Id* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
id_set(Id* self, Uuid* id_table, uint32_t id_part)
{
	self->id_table  = *id_table;
	self->id_part   =  id_part;
	self->id_page   = UINT32_MAX;
	self->id_column = UINT32_MAX;
}

static inline void
id_set_page(Id* self, uint32_t id)
{
	self->id_page  = id;
}

static inline void
id_set_column(Id* self, uint32_t id)
{
	self->id_column = id;
}

static inline int
id_read(Id* self, char* spec)
{
	// <id_table>.<id_part>.meta
	// <id_table>.<id_part>.<id_column>.meta
	// <id_table>.<id_part>.<id_page>
	// <id_table>.<id_part>.<id_page>.<id_column>
	Str name;
	str_set_cstr(&name, spec);

	// id_table
	Str id_table;
	if (! str_split(&name, &id_table, '.'))
		return -1;
	if (uuid_set_nothrow(&self->id_table, &id_table) == -1)
		return -1;
	str_advance(&name, str_size(&id_table) + 1);

	// id_part
	Str id_part;
	if (! str_split(&name, &id_part, '.'))
		return -1;
	uint64_t value;
	if (str_u64(&id_part, &value) == -1)
		return -1;
	self->id_part = value;
	str_advance(&name, str_size(&id_part) + 1);

	// meta
	if (str_is(&name, "meta", 4))
	{
		self->id_page   = UINT32_MAX;
		self->id_column = UINT32_MAX;
		return 0;
	}

	// id_page or id_column
	Str id;
	if (! str_split(&name, &id, '.'))
		return -1;
	if (str_u64(&id, &value) == -1)
		return -1;
	str_advance(&name, str_size(&id) + 1);

	// meta
	if (str_is(&name, "meta", 4))
	{
		// id is column
		self->id_page   = UINT32_MAX;
		self->id_column = value;
		return 0;
	}
	self->id_page = value;

	// id is page
	Str id_column;
	if (! str_split(&name, &id_column, '.'))
		return -1;
	if (str_u64(&id_column, &value) == -1)
		return -1;
	self->id_column = value;
	str_advance(&name, str_size(&id_column) + 1);
	if (! str_empty(&name))
		return -1;

	return 0;
}

static inline void
id_path(Id* self, char* path, uint64_t checkpoint, bool incomplete)
{
	// <id_table>.<id_part>.meta
	// <id_table>.<id_part>.<id_column>.meta
	// <id_table>.<id_part>.<id_page>
	// <id_table>.<id_part>.<id_page>.<id_column>
	char id_table[UUID_SZ];
	uuid_get(&self->id_table, id_table, sizeof(id_table));

	// <id_table>.<id_part>
	auto len = format(path, PATH_MAX, "{s}/checkpoint{s}/{u64}/{s}.{u32}",
	                  state_directory(),
	                  incomplete ? ".incomplete" : "",
	                  checkpoint,
	                  id_table, self->id_part);

	// meta
	if (self->id_page == UINT32_MAX)
	{
		// <id_table>.<id_part>.meta
		// <id_table>.<id_part>.<id_column>.meta
		if (self->id_column != UINT32_MAX)
			len += format(path + len, PATH_MAX - len, ".{u32}", self->id_column);
		format(path + len, PATH_MAX - len, "{s}", ".meta");
		return;
	}

	// <id_page>
	len += format(path + len, PATH_MAX - len, ".{u32}", self->id_page);

	// <id_column>
	if (self->id_column != UINT32_MAX)
		format(path + len, PATH_MAX - len, ".{u32}", self->id_column);
}
