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
	HeapIterator it;
	Str*         key;
	Buf          data;
	Task*        part_task;
	Part*        part;
	Feeds*       feeds;
	List         link;
};

void feed_init(Feed*, Feeds*, Task*, Part*);
void feed_free(Feed*);
void feed_request(Feed*);
void feed_reply(Feed*);
void feed_cancel(Feed*);
void feed_cancel_reply(Feed*);
