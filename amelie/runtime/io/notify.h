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

typedef struct Notify Notify;

struct Notify
{
	volatile int signaled;
	Fd           fd;
	Poller*      poller;
};

void notify_init(Notify*);
int  notify_open(Notify*, Poller*);
void notify_close(Notify*);
void notify_read(Notify*);
void notify_write(Notify*);
