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

typedef struct Streams Streams;

struct Streams
{
	Stream* streams;
	int     streams_count;
	List    ready;
	Buf     key;
	Event   notify;
	Iov     iov;
	Client* client;
	Task*   task;
};

void streams_init(Streams*, Task*, Client*);
void streams_free(Streams*);
void streams_create(Streams*, Parts*, Str*);
void streams_run(Streams*);
