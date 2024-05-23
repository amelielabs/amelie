#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

static inline void
poll_on_read_event(Fd* fd)
{
	Event* on_read = fd->on_read_arg;
	event_signal(on_read);
}

static inline void
poll_read(Fd* fd, int time_ms)
{
	cancellation_point();

	auto poller = &so_task->poller;
	Event on_read;
	event_init(&on_read);
	int rc;
	rc = poller_read(poller, fd, poll_on_read_event, &on_read);
	if (unlikely(rc == -1))
		error_system();
	event_wait(&on_read, time_ms);
	poller_read(poller, fd, NULL, NULL);

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

	auto poller = &so_task->poller;
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
