
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

#include <amelie_runtime>
#include <amelie_type.h>
#include <amelie_storage.h>

void
storage_init(Storage* self, Retention* retention, Id* id, int type)
{
	self->current    = NULL;
	self->meta       = NULL;
	self->list_count = 0;
	self->retention  = retention;
	self->type       = type;
	self->id         = *id;
	self->id_first   = 0;
	self->id_next    = 0;
	self->size       = 2 * 1024 * 1024;
	buf_init(&self->list);
}

void
storage_free(Storage* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto page = storage_at(self, i);
		page_free(page);
	}
	if (self->meta)
		page_free(self->meta);
	buf_free(&self->list);
}

Page*
storage_add_meta(Storage* self, int size)
{
	assert(! self->meta);
	// create and set meta page
	auto page = page_allocate(sizeof(Page) + size);
	page->type       = PAGE_META;
	page->id         = self->id;
	page->id.id_page = UINT32_MAX;
	page->position   = sizeof(Page) + size;
	self->meta       = page;
	return page;
}

Page*
storage_add(Storage* self)
{
	// create new page
	auto page = page_allocate(self->size);
	page->type       = self->type;
	page->id         = self->id;
	page->id.id_page = self->id_next++;
	self->current    = page;

	// assign global page id
	if (self->retention)
		page->id_serial = retention_attach(self->retention);

	buf_write(&self->list, &page, sizeof(Page*));
	self->list_count++;
	return page;
}

void
storage_open(Storage* self, uint64_t checkpoint,
             Ids*     ids,
             Id*      filter)
{
	auto retention = self->retention;

	storage_free(self);
	storage_init(self, retention, &self->id, self->type);

	auto list = buf_create();
	defer_buf(list);
	auto meta = ids_collect(ids, list, filter);

	// load meta page
	self->meta = page_load(meta, checkpoint);

	// load pages (list is sorted)
	uint32_t id = 0;
	auto pos = (Id**)list->start;
	auto end = (Id**)list->position;
	while (pos < end)
	{
		auto page = page_load(*pos, checkpoint);

		buf_write(&self->list, &page, sizeof(Page*));
		self->list_count++;

		if (self->list_count == 1)
			self->id_first = page->id.id_page;

		if (page->id.id_page > id)
			id = page->id.id_page;

		// restore retention state
		if (retention)
			retention_follow(retention, page->id_serial);

		// last page
		self->current = page;
		pos++;
	}

	// set next page id
	if (self->list_count > 0)
		id++;
	self->id_next = id;
}

static inline Page*
storage_pop(Storage* self)
{
	auto page = storage_at(self, 0);
	self->list_count--;

	auto to_move = self->list_count * sizeof(Page*);
	memmove(self->list.start, self->list.start + sizeof(Page*), to_move);
	buf_truncate(&self->list, sizeof(Page*));

	if (self->list_count > 0)
	{
		self->id_first = ((Page**)self->list.start)[0]->id.id_page;
	} else
	{
		self->id_first = 0;
		self->id_next  = 0;
	}
	return page;
}

bool
storage_retention(Storage* self, uint32_t* id)
{
	if (! self->retention)
		return false;

	*id = retention_run(self->retention);
	if (likely(*id  == 0))
		return false;

	// drop all pages < id
	auto changed = false;
	while (self->list_count > 1)
	{
		auto first = storage_at(self, 0);
		if (first->id_serial >= *id)
			break;
		auto page = storage_pop(self);
		assert(page);
		page_free(page);
		changed = true;
	}

	return changed;
}
