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
	atomic_u32   signal;
	int          fd;
	IoEvent      event;
	uint64_t     event_data;
	struct iovec iov;
	Io*          io;
};

void notify_init(Notify*);
int  notify_open(Notify*, Io*, IoFunction, void*);
void notify_close(Notify*);
void notify_read_signal(Notify*);
void notify_read(Notify*);
void notify_write(Notify*);
