
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
pages_init(Pages*   self, PageMgr* mgr, Retention* retention,
           Uuid*    id_rel,
           uint32_t id_column,
           int      type_meta,
           int      type)
{
	self->current   = NULL;
	self->meta      = NULL;
	self->count     = 0;
	self->id        = 0;
	self->id_first  = 0;
	self->id_rel    = id_rel;
	self->id_column = id_column;
	self->type_meta = type_meta;
	self->type      = type;
	self->retention = retention;
	self->mgr       = mgr;
	buf_init(&self->list);
}

void
pages_free(Pages* self)
{
	auto mgr = self->mgr;
	for (auto i = 0; i < self->count; i++)
	{
		auto page = pages_at(self, i);
		page_mgr_push(mgr, page);
	}
	if (self->meta)
		page_mgr_push(mgr, self->meta);
	buf_free(&self->list);
}

Page*
pages_add_meta(Pages* self)
{
	// create and set meta page
	assert(! self->meta);
	auto page = page_mgr_pop(self->mgr);
	page->type      = self->type_meta;
	page->id        = UINT32_MAX;
	page->id_rel    = *self->id_rel;
	page->id_column = self->id_column;
	page->position  = sizeof(Page);
	self->meta      = page;
	return page;
}

Page*
pages_add(Pages* self)
{
	// create new page
	auto page = page_mgr_pop(self->mgr);
	page->type      = self->type;
	page->id        = self->id++;
	page->id_rel    = *self->id_rel;
	page->id_column = self->id_column;
	self->current = page;

	buf_write(&self->list, &page, sizeof(Page*));
	self->count++;
	return page;
}

void
pages_import(Pages* self, Page* page)
{
	if (page->type == PAGE_HEAP_META ||
	    page->type == PAGE_FLAT_META)
	{
		// (from initial creation)
		if (self->meta)
			page_mgr_push(self->mgr, self->meta);
		self->meta = page;
		return;
	}
	buf_write(&self->list, &page, sizeof(Page*));
	self->count++;
}

hot static int
pages_cmp(const void* p1, const void* p2, void* arg)
{
	unused(arg);
	auto a = *(Page**)p1;
	auto b = *(Page**)p2;
	return compare_uint32(a->id, b->id);
}

void
pages_import_done(Pages* self)
{
	if (! self->count)
	{
		self->id_first = 0;
		self->id       = 0;
		return;
	}

	// sort pages by id
	qsort_r(self->list.start, self->count, sizeof(Page*),
	        pages_cmp, self);

	// set first id
	self->id_first = pages_at(self, 0)->id;

	// set last
	self->current = pages_at(self, self->count - 1);
	self->id = self->current->id + 1;
}

static inline Page*
pages_pop(Pages* self)
{
	auto page = pages_at(self, 0);
	self->count--;

	auto to_move = self->count * sizeof(Page*);
	memmove(self->list.start, self->list.start + sizeof(Page*), to_move);
	buf_truncate(&self->list, sizeof(Page*));

	if (self->count > 0)
	{
		self->id_first = ((Page**)self->list.start)[0]->id;
	} else
	{
		self->id_first = 0;
		self->id       = 0;
	}
	return page;
}

bool
pages_retention(Pages* self, uint32_t* id)
{
	if (! self->retention)
		return false;

	int count = self->retention->count;
	if (count == INT_MAX)
		return false;

	// everything < watermark considered for removal
	*id = 0;
	if (unlikely(self->count > count))
	{
		assert((int)self->id >= count);
		*id = self->id - count;
	}
	if (! *id)
		return false;

	// drop all pages < id
	auto changed = false;
	while (self->count > 1)
	{
		auto first = pages_at(self, 0);
		if (first->id >= *id)
			break;
		auto page = pages_pop(self);
		assert(page);
		page_mgr_push(self->mgr, page);
		changed = true;
	}

	return changed;
}
