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

static inline void
poll_on_read_event(Fd* fd)
{
	Event* on_read = fd->on_read_arg;
	event_signal(on_read);
}

static inline void
poll_read_start(Fd* fd, Event* on_read)
{
	auto poller = &am_task->poller;
	auto rc = poller_read(poller, fd, poll_on_read_event, on_read);
	if (unlikely(rc == -1))
		error_system();
}

static inline void
poll_read_stop(Fd* fd)
{
	auto poller = &am_task->poller;
	poller_read(poller, fd, NULL, NULL);
}

hot static inline void
poll_read(Fd* fd, int time_ms)
{
	cancellation_point();

	Event on_read;
	event_init(&on_read);
	poll_read_start(fd, &on_read);
	event_wait(&on_read, time_ms);
	poll_read_stop(fd);

	cancellation_point();
}

static inline void
poll_on_write_event(Fd* fd)
{
	Event *on_write = fd->on_write_arg;
	event_signal(on_write);
}

static inline void
poll_write(Fd* fd, int time_ms)
{
	cancellation_point();

	auto poller = &am_task->poller;
	Event on_write;
	event_init(&on_write);
	int rc;
	rc = poller_write(poller, fd, poll_on_write_event, &on_write);
	if (unlikely(rc == -1))
		error_system();
	event_wait(&on_write, time_ms);
	poller_write(poller, fd, NULL, NULL);

	cancellation_point();
}
