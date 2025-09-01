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

typedef struct IoEvent IoEvent;
typedef struct Io      Io;

typedef void (*IoFunction)(IoEvent*);

struct IoEvent
{
	ssize_t    rc;
	Event      event;
	IoFunction callback;
	void*      callback_arg;
};

struct Io
{
	struct io_uring* ring;
	bool             pending;
};

static inline void
io_event_init(IoEvent* self)
{
	self->rc           = 0;
	self->callback     = NULL;
	self->callback_arg = NULL;
	event_init(&self->event);
}

static inline void
io_init(Io* self)
{
	self->ring    = NULL;
	self->pending = false;
}

static inline int
io_create(Io* self)
{
	self->ring = (struct io_uring*)am_malloc(sizeof(*self->ring));
	if (io_uring_queue_init(1024, self->ring, 0) < 0)
		return -1;
	return 0;
}

static inline void
io_free(Io* self)
{
	if (self->ring)
	{
		io_uring_queue_exit(self->ring);
		am_free(self->ring);
		self->ring = NULL;
	}
}

static inline void
io_set_pending(Io* self)
{
	self->pending = true;
}

always_inline static inline void
io_process_event(Io* self, struct io_uring_cqe* cqe)
{
	auto event = (IoEvent*)cqe->user_data;
	event->rc = cqe->res;
	event_signal_local(&event->event);
	io_uring_cqe_seen(self->ring, cqe);
	if (event->callback)
		event->callback(event);
}

hot static inline int
io_process(Io* self)
{
	int count = 0;
	struct io_uring_cqe* cqe;
	while (io_uring_peek_cqe(self->ring, &cqe) == 0)
	{
		io_process_event(self, cqe);
		count++;
	}
	return count;
}

hot static inline void
io_step(Io* self, uint32_t time_ms)
{
	// process delayed submit
	if (self->pending)
	{
		io_uring_submit(self->ring);
		self->pending = false;
	}

	// process pending events
	if (io_process(self) > 0)
		return;

	// wait for next events
	struct io_uring_cqe* cqe;
	if (time_ms == UINT32_MAX)
	{
		if (io_uring_wait_cqe(self->ring, &cqe) == 0)
			io_process_event(self, cqe);
	} else
	{
		struct __kernel_timespec ts;
		ts.tv_sec  = time_ms / 1000;
		ts.tv_nsec = (time_ms % 1000) * 1000000;
		if (io_uring_wait_cqe_timeout(self->ring, &cqe, &ts) == 0)
			io_process_event(self, cqe);
	}
	io_process(self);
}
