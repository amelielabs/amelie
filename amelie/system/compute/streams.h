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
	Stream* stream;
	List    ready;
	Event   notify;
	Iov     iov;
	Client* client;
	Task*   task;
};

void streams_init(Streams*, Task*, Client*);
void streams_free(Streams*);
void streams_create_for(Streams*, Part*, Timeline*, Str*);
void streams_create(Streams*, Part*, Timeline*);
void streams_run(Streams*);
