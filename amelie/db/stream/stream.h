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

typedef struct StreamEvent  StreamEvent;
typedef struct StreamHeader StreamHeader;
typedef struct Stream       Stream;

struct StreamEvent
{
	uint64_t id;
	uint32_t data_size;
	uint8_t  data[];
} packed;

struct StreamHeader
{
	uint64_t id;
} packed;

struct Stream
{
	Spinlock lock;
	int      refs;
	uint64_t id;
	List     subs;
	int      subs_count;
	bool     shutdown;
	Storage  storage;
};

Stream*
stream_allocate(void);
void   stream_ref(Stream*);
void   stream_unref(Stream*);
void   stream_shutdown(Stream*);
size_t stream_create(Stream*, char*);
size_t stream_open(Stream*, char*);
void   stream_gc(Stream*);
void   stream_subscribe(Stream*, StreamSub*);
void   stream_unsubscribe(Stream*, StreamSub*);
void   stream_write(Stream*, uint8_t*, uint32_t);
void   stream_state(Stream*, Buf*);
size_t stream_size(Stream*);
