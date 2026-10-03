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
pages_add(Pages* self, PageId* id)
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

		PageId id;
		page_id_init(&id);
		if (page_id_read(&id, entry->d_name) == -1)
			continue;
		pages_add(self, &id);
	}
}
