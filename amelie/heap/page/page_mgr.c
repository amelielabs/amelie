
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
#include <amelie_page.h>

void
page_mgr_init(PageMgr* self)
{
	self->count      = 0;
	self->count_free = 0;
	self->size_page  = 64 * 1024;
	buf_init(&self->list);
	buf_init(&self->list_free);
}

void
page_mgr_free(PageMgr* self)
{
	for (auto i = 0; i < self->count; i++)
	{
		void* pointer = page_mgr_at(self, i);
		vfs_munmap(pointer, self->size_page);
	}
	buf_free(&self->list);
	buf_free(&self->list_free);
}

static void
page_mgr_allocate(PageMgr* self)
{
	// allocate page
	auto prot = PROT_READ|PROT_WRITE;
	uint8_t* pointer = mmap(NULL, self->size_page, prot, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	if (unlikely(pointer == MAP_FAILED))
		error_system();

	auto page = (Page*)(pointer);
	memset(page, 0, sizeof(Page));
	page->free          = true;
	page->size          = self->size_page;
	page->position      = sizeof(Page);
	page->position_last = page->position;

	buf_write(&self->list, &page, sizeof(Page**));
	buf_write(&self->list_free, &page, sizeof(Page**));
	self->count++;
	self->count_free++;
}

Page*
page_mgr_pop(PageMgr* self)
{
	if (unlikely(! self->count_free))
		page_mgr_allocate(self);

	auto page = page_mgr_at_free(self, self->count_free - 1);
	buf_truncate(&self->list_free, sizeof(Page**));
	self->count_free--;

	page->free  = false;
	page->dirty = true;
	return page;
}

void
page_mgr_push(PageMgr* self, Page* page)
{
	page->type          = PAGE_UNDEF;
	page->free          = true;
	page->dirty         = false;
	page->id            = 0;
	page->id_column     = UINT32_MAX;
	page->position      = sizeof(Page);
	page->position_last = page->position;

	uuid_init(&page->id_rel);
	buf_write(&self->list_free, &page, sizeof(Page**));
	self->count_free++;
}

void
page_mgr_status(PageMgr* self, Buf* buf)
{
	// {}
	encode_obj(buf);

	// usage
	encode_raw(buf, "usage_mb", 8);
	encode_int(buf, (self->count * self->size_page) / 1024 / 1024);

	// usage
	encode_raw(buf, "usage_kb", 8);
	encode_int(buf, (self->count * self->size_page) / 1024);

	// pages
	encode_raw(buf, "pages", 5);
	encode_int(buf, self->count);

	// pages_free
	encode_raw(buf, "pages_free", 10);
	encode_int(buf, self->count_free);

	encode_obj_end(buf);
}
