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

hot static inline int
io_process(Io* self)
{
	unsigned int count = 0;
	unsigned int it;
	struct io_uring_cqe* cqe;
	io_uring_for_each_cqe(self->ring, it, cqe)
	{
		auto event = (IoEvent*)cqe->user_data;
		event->rc = cqe->res;
		event_signal_local(&event->event);
		if (event->callback)
			event->callback(event);
		count++;
	}
	io_uring_cq_advance(self->ring, count);
	return count;
}

hot static inline void
io_step(Io* self, uint32_t time_ms)
{
	// process pending events
	if (io_process(self) > 0)
	{
		// submit pending events
		if (self->pending)
		{
			io_uring_submit(self->ring);
			self->pending = false;
		}
		return;
	}

	struct io_uring_cqe* cqe;
	if (self->pending)
	{
		// submit and wait
		if (time_ms == UINT32_MAX)
		{
			io_uring_submit_and_wait(self->ring, 1);
		} else
		{
			struct __kernel_timespec ts;
			ts.tv_sec  = time_ms / 1000;
			ts.tv_nsec = (time_ms % 1000) * 1000000;
			io_uring_submit_and_wait_timeout(self->ring, &cqe, 1, &ts, NULL);
		}
		self->pending = false;
	} else
	{
		// wait
		if (time_ms == UINT32_MAX)
		{
			io_uring_wait_cqe(self->ring, &cqe);
		} else
		{
			struct __kernel_timespec ts;
			ts.tv_sec  = time_ms / 1000;
			ts.tv_nsec = (time_ms % 1000) * 1000000;
			io_uring_wait_cqe_timeout(self->ring, &cqe, &ts);
		}
	}
	io_process(self);
}
