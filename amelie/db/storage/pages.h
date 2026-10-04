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

typedef struct Pages Pages;

struct Pages
{
	Buf list;
	int list_count;
};

static inline void
pages_init(Pages* self)
{
	self->list_count = 0;
	buf_init(&self->list);
}

static inline void
pages_free(Pages* self)
{
	buf_free(&self->list);
}

static inline void
pages_add(Pages* self, Id* id)
{
	buf_write(&self->list, id, sizeof(*id));
	self->list_count++;
}

static inline void
pages_read(Pages* self, uint64_t checkpoint)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}",
	       state_directory(), checkpoint);

	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	defer(fs_closedir_defer, dir);

	// <id_table>.<id_part>.meta
	// <id_table>.<id_part>.<id_column>.meta
	// <id_table>.<id_part>.<id_page>
	// <id_table>.<id_part>.<id_page>.<id_column>
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (! strcmp(entry->d_name, "."))
			continue;
		if (! strcmp(entry->d_name, ".."))
			continue;

		Id id;
		id_init(&id);
		if (id_read(&id, entry->d_name) == -1)
			continue;
		pages_add(self, &id);
	}
}

static inline Id*
pages_collect(Pages* self, Buf* list, Uuid* id_table, int id_part, int id_column)
{
	Id* meta = NULL;
	(void)self;
	(void)id_table;
	(void)id_part;
	(void)id_column;
	(void)list;

	// todo: match everything matching id_table and id_part without id_column
	// todo: sort by id_part
	return meta;
}
