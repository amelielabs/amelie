
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
storage_init(Storage* self, Id* id, int type)
{
	self->current    = NULL;
	self->meta       = NULL;
	self->meta_fd    = -1;
	self->list_count = 0;
	self->type       = type;
	self->id         = *id;
	self->id_first   = 0;
	self->id_next    = 0;
	self->size       = 64 * 1024 * 1024;
	buf_init(&self->list);
	buf_init(&self->list_fd);
}

void
storage_free(Storage* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto page = storage_at(self, i);
		auto fd   = storage_at_fd(self, i);
		page_free(page);
		if (*fd != -1)
		{
			close(*fd);
			*fd = -1;
		}
	}
	if (self->meta)
		page_free(self->meta);
	if (self->meta_fd != -1)
	{
		close(self->meta_fd);
		self->meta_fd = -1;
	}
	buf_free(&self->list);
	buf_free(&self->list_fd);
}

Page*
storage_add_meta(Storage* self, int size)
{
	assert(! self->meta);
	// create and set meta page
	auto page = page_allocate(sizeof(Page) + size, &self->meta_fd);
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
	int  page_fd = -1;
	auto page = page_allocate(self->size, &page_fd);
	page->type       = self->type;
	page->id         = self->id;
	page->id.id_page = self->id_next++;
	self->current    = page;

	buf_write(&self->list, &page, sizeof(Page*));
	buf_write(&self->list_fd, &page_fd, sizeof(int));
	self->list_count++;
	return page;
}

void
storage_open(Storage* self, uint64_t checkpoint,
             Id*      meta,
             Buf*     list)
{
	storage_free(self);
	storage_init(self, &self->id, self->type);

	// load meta page
	self->meta = page_load(meta, &self->meta_fd, checkpoint);

	// load pages (list is sorted)
	uint32_t id = 0;
	auto pos = (Id**)list->start;
	auto end = (Id**)list->position;
	while (pos < end)
	{
		int  page_fd = -1;
		auto page = page_load(*pos, &page_fd, checkpoint);

		buf_write(&self->list, &page, sizeof(Page*));
		buf_write(&self->list_fd, &page_fd, sizeof(int));
		self->list_count++;

		if (self->list_count == 1)
			self->id_first = page->id.id_page;

		if (page->id.id_page > id)
			id = page->id.id_page;

		// last page
		self->current = page;
		pos++;
	}

	// set next page id
	if (self->list_count > 0)
		id++;
	self->id_next = id;
}
