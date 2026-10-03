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

typedef struct PageId PageId;

struct PageId
{
	Uuid     id_table;
	uint32_t id_part;
	uint32_t id_page;
	uint32_t id_column;
} packed;

static inline void
page_id_init(PageId* self)
{
	memset(self, 0, sizeof(*self));
	self->id_column = UINT32_MAX;
}

static inline int
page_id_read(PageId* self, char* spec)
{
	(void)self;
	(void)spec;
	// todo:
	return 0;
}

static inline void
page_id_path(PageId* self, char* path, uint64_t checkpoint, bool incomplete)
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
