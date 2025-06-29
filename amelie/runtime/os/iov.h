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

typedef struct Iov Iov;

struct Iov
{
	Buf iov;
	int iov_count;
	int size;
};

static inline void
iov_init(Iov* self)
{
	self->size  = 0;
	self->iov_count = 0;
	buf_init(&self->iov);
}

static inline void
iov_free(Iov* self)
{
	buf_free(&self->iov);
}

static inline void
iov_reset(Iov* self)
{
	self->size = 0;
	self->iov_count = 0;
	buf_reset(&self->iov);
}

static inline struct iovec*
iov_pointer(Iov* self)
{
	return (struct iovec*)self->iov.start;
}

static inline void
iov_reserve(Iov* self, int count)
{
	buf_reserve(&self->iov, sizeof(struct iovec) * count);
}

static inline void
iov_add(Iov* self, void* pointer, int size)
{
	buf_reserve(&self->iov, sizeof(struct iovec));
	auto iov = (struct iovec*)self->iov.position;
	iov->iov_base = pointer;
	iov->iov_len  = size;
	buf_advance(&self->iov, sizeof(struct iovec));
	self->iov_count++;
	self->size += size;
}

static inline void
iov_add_buf(Iov* self, Buf* buf)
{
	iov_add(self, buf->start, buf_size(buf));
}
