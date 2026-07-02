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

typedef struct Bufs Bufs;

struct Bufs
{
	Spinlock lock;
	Buf**    stack;
	int      stack_count;
	int      stack_size;
};

static inline void
bufs_init(Bufs* self)
{
	self->stack       = NULL;
	self->stack_count = 0;
	self->stack_size  = 0;
	spinlock_init(&self->lock);
}

static inline int
bufs_allocate_nothrow(Bufs* self, int capacity)
{
	assert(! self->stack);
	self->stack_size = capacity;
	self->stack      =
		am_malloc_aligned_nothrow(capacity * sizeof(Buf*), cache_line);
	if (! self->stack)
		return -1;
	return 0;
}

static inline void
bufs_free(Bufs* self)
{
	if (self->stack)
	{
		while (self->stack_count > 0)
		{
			auto buf = self->stack[self->stack_count - 1];
			self->stack_count--;
			buf_free_memory(buf);
		}
		am_free(self->stack);
	}
	spinlock_free(&self->lock);
	bufs_init(self);
}

static inline int
bufs_pop(Bufs* self, Buf** batch, int count)
{
	spinlock_lock(&self->lock);

	if (self->stack_count < count)
		count = self->stack_count;

	if (count > 0)
	{
		auto at = &self->stack[self->stack_count - count];
		memcpy(batch, at, count * sizeof(Buf*));
		self->stack_count -= count;
	}

	spinlock_unlock(&self->lock);
	return count;
}

static inline void
bufs_push(Bufs* self, Buf** batch, int count)
{
	auto total = count;
	spinlock_lock(&self->lock);

	if ((self->stack_count + count) > self->stack_size)
		count = self->stack_size - self->stack_count;

	if (count > 0)
	{
		auto at = &self->stack[self->stack_count];
		memcpy(at, batch, count * sizeof(Buf*));
		self->stack_count += count;
	}

	spinlock_unlock(&self->lock);

	// free the rest
	if ((total - count) > 0)
	{
		for (auto i = count; i < total; i++)
			buf_free_memory(batch[i]);
	}
}
