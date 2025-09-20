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

struct IoEvent
{
	ssize_t rc;
	Event   event;
};

struct Io
{
	struct io_uring ring;
	bool            pending;
	atomic_u32      pending_wakeup;
	atomic_u32      sleep;
	IoEvent         wakeup;
};

static inline void
io_event_init(IoEvent* self)
{
	self->rc = 0;
	event_init(&self->event);
}

static inline void
io_init(Io* self)
{
	self->pending        = false;
	self->pending_wakeup = 0;
	self->sleep          = 0;
	io_event_init(&self->wakeup);
	memset(&self->ring, 0, sizeof(self->ring));
}

static inline int
io_create(Io* self)
{
	if (io_uring_queue_init(2048, &self->ring, 0) < 0)
		return -1;
	return 0;
}

static inline void
io_free(Io* self)
{
	io_uring_queue_exit(&self->ring);
}

static inline void
io_set_pending(Io* self)
{
	self->pending = true;
}

static inline void
io_set_sleep(Io* self, int value)
{
	atomic_u32_set(&self->sleep, value);
}

hot static inline void
io_wakeup(Io* self)
{
	if (! atomic_u32_of(&self->sleep))
		return;
	if (__sync_lock_test_and_set(&self->pending_wakeup, 1) == 1)
		return;

	auto sqe = io_uring_get_sqe(&self->ring);
	assert(sqe);
	// FIXME
	sqe->user_data = (uintptr_t)&self->wakeup;
	io_uring_prep_nop(sqe);

	io_uring_submit(&self->ring);
}

hot static inline int
io_process(Io* self)
{
	unsigned int count = 0;
	unsigned int it;
	struct io_uring_cqe* cqe;
	io_uring_for_each_cqe(&self->ring, it, cqe)
	{
		auto event = (IoEvent*)cqe->user_data;
		event->rc = cqe->res;
		event_signal_local(&event->event);
		if (event == &self->wakeup)
			__sync_lock_release(&self->pending_wakeup);
		count++;
	}
	if (count > 0)
		io_uring_cq_advance(&self->ring, count);
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
			io_uring_submit(&self->ring);
			self->pending = false;
		}
		return;
	}

	io_set_sleep(self, 1);

	struct io_uring_cqe* cqe;
	if (self->pending)
	{
		// submit and wait
		if (time_ms == UINT32_MAX)
		{
			io_uring_submit_and_wait(&self->ring, 1);
		} else
		{
			struct __kernel_timespec ts;
			ts.tv_sec  = time_ms / 1000;
			ts.tv_nsec = (time_ms % 1000) * 1000000;
			io_uring_submit_and_wait_timeout(&self->ring, &cqe, 1, &ts, NULL);
		}
		self->pending = false;
	} else
	{
		// wait
		if (time_ms == UINT32_MAX)
		{
			io_uring_wait_cqe(&self->ring, &cqe);
		} else
		{
			struct __kernel_timespec ts;
			ts.tv_sec  = time_ms / 1000;
			ts.tv_nsec = (time_ms % 1000) * 1000000;
			io_uring_wait_cqe_timeout(&self->ring, &cqe, &ts);
		}
	}
	io_set_sleep(self, 0);

	io_process(self);
}
