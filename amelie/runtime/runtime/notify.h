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

typedef void (*NotifyFunction)(void*);

struct Notify
{
	volatile int   signaled;
	Fd             fd;
	NotifyFunction on_notify;
	void*          arg;
	Poller*        poller;
};

void notify_init(Notify*);
int  notify_open(Notify*, Poller*, NotifyFunction, void*);
void notify_close(Notify*);
void notify_signal(Notify*);
