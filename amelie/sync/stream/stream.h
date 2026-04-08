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

typedef struct StreamPage  StreamPage;
typedef struct StreamEvent StreamEvent;
typedef struct Stream      Stream;

struct StreamPage
{
	List     link;
	uint32_t pos;
	uint32_t end;
	uint8_t  data[];
};

struct StreamEvent
{
	uint64_t lsn;
	uint32_t data_size;
	uint8_t  data[];
} packed;

struct Stream
{
	Spinlock    lock;
	uint64_t    lsn;
	bool        shutdown;
	StreamPage* current;
	List        waiters;
	int         waiters_count;
	List        pages;
	int         pages_count;
	List        slots;
	int         slots_count;
};

Stream*
stream_allocate(void);
void stream_free(Stream*);
void stream_attach(Stream*, StreamSlot*);
void stream_detach(Stream*, StreamSlot*);
void stream_shutdown(Stream*);
void stream_gc(Stream*);
void stream_write(Stream*, uint64_t, uint8_t*, uint32_t);
