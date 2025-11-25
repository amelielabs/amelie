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

typedef struct BookmarkMgr BookmarkMgr;

struct BookmarkMgr
{
	List list;
	int  list_count;
};

void bookmark_mgr_init(BookmarkMgr*);
void bookmark_mgr_free(BookmarkMgr*);
void bookmark_mgr_open(BookmarkMgr*, const char*);
void bookmark_mgr_sync(BookmarkMgr*, const char*);
void bookmark_mgr_add(BookmarkMgr*, Bookmark*);
void bookmark_mgr_delete(BookmarkMgr*, Str*);
Bookmark*
bookmark_mgr_find(BookmarkMgr*, Str*);
