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

typedef struct Bus Bus;

struct Bus
{
	Spinlock   lock;
	List       list;
	atomic_u64 list_count;
	Io*        io;
};

void bus_init(Bus*, Io*);
void bus_free(Bus*);
void bus_attach(Bus*, Event*);
void bus_detach(Event*);
void bus_signal(Event*);
void bus_step(Bus*);

static inline uint64_t
bus_pending(Bus* self)
{
	return atomic_u64_of(&self->list_count);
}
