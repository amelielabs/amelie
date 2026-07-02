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

typedef struct Bookmarks Bookmarks;

struct Bookmarks
{
	List list;
	int  list_count;
};

void bookmarks_init(Bookmarks*);
void bookmarks_free(Bookmarks*);
void bookmarks_open(Bookmarks*, const char*);
void bookmarks_sync(Bookmarks*, const char*);
void bookmarks_add(Bookmarks*, Bookmark*);
void bookmarks_delete(Bookmarks*, Str*);
Bookmark*
bookmarks_find(Bookmarks*, Str*);
