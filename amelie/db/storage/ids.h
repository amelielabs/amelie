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

typedef struct Ids Ids;

struct Ids
{
	Buf list;
	int list_count;
};

static inline void
ids_init(Ids* self)
{
	self->list_count = 0;
	buf_init(&self->list);
}

static inline void
ids_free(Ids* self)
{
	buf_free(&self->list);
}

static inline void
ids_add(Ids* self, Id* id)
{
	buf_write(&self->list, id, sizeof(*id));
	self->list_count++;
}

static inline void
ids_read(Ids* self, uint64_t checkpoint)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}",
	       state_directory(), checkpoint);

	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	defer(fs_closedir_defer, dir);

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
		ids_add(self, &id);
	}
}

hot static int
ids_cmp(const void* p1, const void* p2)
{
	auto a = (*(Id**)p1)->id_page;
	auto b = (*(Id**)p2)->id_page;
	return compare_uint64(a, b);
}

static inline Id*
ids_collect(Ids* self, Buf* list, Id* filter)
{
	auto meta = (Id*)NULL;
	auto pos  = (Id*)self->list.start;
	auto end  = (Id*)self->list.position;
	for (; pos < end; pos++)
	{
		if (! uuid_is(&filter->id_table, &pos->id_table))
			continue;
		if (filter->id_part != pos->id_part)
			continue;
		if (filter->id_column != pos->id_column)
			continue;
		// meta
		if (pos->id_page == UINT32_MAX)
		{
			meta = pos;
			continue;
		}
		buf_write(list, &pos, sizeof(Id**));
	}

	// sort by page
	if (! buf_empty(list))
	{
		auto count = buf_size(list) / sizeof(Id**);
		qsort(list->start, count, sizeof(Id**), ids_cmp);
	}

	return meta;
}
