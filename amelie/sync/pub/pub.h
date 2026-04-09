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

typedef struct PubPage  PubPage;
typedef struct PubEvent PubEvent;
typedef struct Pub      Pub;

struct PubPage
{
	List     link;
	uint32_t pos;
	uint32_t end;
	uint8_t  data[];
};

struct PubEvent
{
	uint64_t lsn;
	uint32_t data_size;
	uint8_t  data[];
} packed;

struct Pub
{
	Spinlock lock;
	uint64_t lsn;
	bool     shutdown;
	Uuid     id;
	PubPage* current;
	List     waiters;
	int      waiters_count;
	List     pages;
	int      pages_count;
	List     slots;
	int      slots_count;
	List     link;
};

Pub* pub_allocate(Uuid*);
void pub_free(Pub*);
void pub_attach(Pub*, PubSlot*);
void pub_detach(Pub*, PubSlot*);
void pub_shutdown(Pub*);
void pub_gc(Pub*);
void pub_write(Pub*, uint64_t, uint8_t*, uint32_t);
