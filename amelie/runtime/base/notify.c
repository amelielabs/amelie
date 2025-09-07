
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

#include <amelie_base.h>

void
notify_init(Notify* self)
{
	self->signal     = 0;
	self->fd         = -1;
	self->event_data = 0;
	io_event_init(&self->event);
	memset(&self->iov, 0, sizeof(self->iov));
}

int
notify_open(Notify* self, Io* io, IoFunction fn, void* fn_arg)
{
	self->fd = eventfd(0, 0);
	if (unlikely(self->fd == -1))
		return -1;
	self->signal     = 0;
	self->event_data = 0;
	self->io         = io;
	self->event.callback = fn;
	self->event.callback_arg = fn_arg;

	auto iov = &self->iov;
	iov->iov_base = &self->event_data;
	iov->iov_len  = sizeof(self->event_data);
	return 0;
}

void
notify_close(Notify* self)
{
	if (self->fd == -1)
		return;
	close(self->fd);
	self->fd = -1;
}

void
notify_read(Notify* self)
{
	auto sqe = io_uring_get_sqe(self->io->ring);
	assert(sqe);
	sqe->user_data = (uintptr_t)&self->event;
	io_uring_prep_readv(sqe, self->fd, &self->iov, 1, 0);
	io_set_pending(self->io);
}

void
notify_read_signal(Notify* self)
{
	__sync_lock_release(&self->signal);
}

void
notify_write(Notify* self)
{
	if (__sync_lock_test_and_set(&self->signal, 1) == 1)
		return;
	uint64_t id = 1;
	write(self->fd, &id, sizeof(id));
}
