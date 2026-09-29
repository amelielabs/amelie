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

typedef struct Feed  Feed;
typedef struct Feeds Feeds;

struct Feed
{
	Msg          msg;
	Msg          msg_cancel;
	bool         ready;
	bool         cancel;
	bool         wait;
	bool         error;

	HeapIterator it;
	Row*         key;
	Buf          data;

	Task*        part_task;
	Part*        part;
	Feed*        part_link;
	Task*        task;

	Feeds*       feeds;
	List         link;
};

void feed_init(Feed*, Feeds*, Task*, Task*, Part*);
void feed_free(Feed*);
void feed_next(Feed*);
void feed_cancel(Feed*);
void feed_cancel_all(Part*);
void feed_resume(Part*);
