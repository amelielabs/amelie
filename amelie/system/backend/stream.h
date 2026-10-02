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

typedef struct Stream  Stream;
typedef struct Streams Streams;

struct Stream
{
	Msg          msg;
	Msg          msg_cancel;
	bool         wait;
	bool         shutdown;
	bool         ready;
	Event        on_complete;
	Event        on_cancel;
	// iterator
	HeapIterator it;
	Row*         key;
	Buf          data;
	Timeline     timeline;
	// partition state
	Task*        part_task;
	Part*        part;
	Stream*      part_link;
	Streams*     streams;
	List         link;
};

void stream_init(Stream*, Streams*, Part*, Timeline*);
void stream_free(Stream*);
void stream_next(Stream*);
void stream_cancel(Stream*);
void streaming_cancel(Part*);
void streaming_resume(Part*);
