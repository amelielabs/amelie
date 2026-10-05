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
	// <id_table>.<id_part>.part
	// <id_table>.<id_part>.part.<id_page>
	// <id_table>.<id_part>.column.<column>
	// <id_table>.<id_part>.column.<column>.<page>
	Str id;
	Str name;
	str_set_cstr(&name, spec);

	// table
	if (! str_split(&name, &id, '.'))
		return -1;
	if (uuid_set_nothrow(&self->id_table, &id) == -1)
		return -1;
	str_advance(&name, str_size(&id) + 1);

	// part
	if (! str_split(&name, &id, '.'))
		return -1;
	uint64_t value;
	if (str_u64(&id, &value) == -1)
		return -1;
	self->id_part = value;
	str_advance(&name, str_size(&id) + 1);

	// type
	str_split(&name, &id, '.');
	str_advance(&name, str_size(&id));

	// partition file
	if (str_is(&id, "part", 4))
	{
		if (str_empty(&name))
		{
			self->id_page   = UINT32_MAX;
			self->id_column = UINT32_MAX;
			return 0;
		}

		// page
		str_advance(&name, 1);
		if (str_u64(&name, &value) == -1)
			return -1;
		self->id_page   = value;
		self->id_column = UINT32_MAX;
		return 0;
	}

	// partition column file
	if (! str_is(&id, "column", 4))
		return -1;
	str_advance(&name, 1);

	// column
	str_split(&name, &id, '.');
	if (str_u64(&id, &value) == -1)
		return -1;
	str_advance(&name, str_size(&id));
	self->id_column = value;
	if (str_empty(&name))
	{
		self->id_page = UINT32_MAX;;
		return 0;
	}
	str_advance(&name, 1);

	// column page
	if (str_u64(&name, &value) == -1)
		return -1;
	self->id_page = value;
	return 0;
}

static inline void
id_path(Id* self, char* path, uint64_t checkpoint, bool incomplete)
{
	// <id_table>.<id_part>.part
	// <id_table>.<id_part>.part.<id_page>
	// <id_table>.<id_part>.column.<column>
	// <id_table>.<id_part>.column.<column>.<page>
	char id_table[UUID_SZ];
	uuid_get(&self->id_table, id_table, sizeof(id_table));

	auto len = format(path, PATH_MAX, "{s}/checkpoint/{u64}{s}/{s}.{u32}",
	                  state_directory(),
	                  checkpoint,
	                  incomplete ? ".incomplete" : "",
	                  id_table, self->id_part);

	// column file
	if (self->id_column != UINT32_MAX)
	{
		if (self->id_page != UINT32_MAX)
			format(path + len, PATH_MAX - len, ".column.{u32}.{u32}",
			       self->id_column, self->id_page);
		else
			format(path + len, PATH_MAX - len, ".column.{u32}",
			       self->id_column);
		return;
	}

	// partition file
	if (self->id_page != UINT32_MAX)
		format(path + len, PATH_MAX - len, ".part.{u32}",
		       self->id_page);
	else
		format(path + len, PATH_MAX - len, ".part");
}
