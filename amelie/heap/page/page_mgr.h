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

typedef struct PageMgr PageMgr;

struct PageMgr
{
	int count;
	int count_free;
	int count_huge;
	int size_page;
	int size_huge;
	Buf list;
	Buf list_free;
	Buf list_huge;
};

void  page_mgr_init(PageMgr*);
void  page_mgr_free(PageMgr*);
Page* page_mgr_pop(PageMgr*);
void  page_mgr_push(PageMgr*, Page*);

static inline Page*
page_mgr_at(PageMgr* self, int order)
{
	return ((Page**)self->list.start)[order];
}

static inline Page*
page_mgr_at_free(PageMgr* self, int order)
{
	return ((Page**)self->list_free.start)[order];
}
