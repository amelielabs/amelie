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
	atomic_u64 signals;
	List       list_ready;
	Notify     notify;
};

void     bus_init(Bus*);
void     bus_free(Bus*);
int      bus_open(Bus*, Io*);
void     bus_close(Bus*);
void     bus_attach(Bus*, Event*);
void     bus_detach(Event*);
void     bus_signal(Event*);
uint64_t bus_step(Bus*);
