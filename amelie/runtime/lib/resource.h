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

typedef struct Resource Resource;

struct Resource
{
	int        readers_count;
	List       readers;
	int        readers_wait_count;
	List       readers_wait;
	Lock*      writer;
	int        writers_wait_count;
	List       writers_wait;
	LockCache* lock_cache;
};

static inline void
resource_init(Resource* self, LockCache* lock_cache)
{
	self->readers_count      = 0;
	self->readers_wait_count = 0;
	self->writers_wait_count = 0;
	self->writer             = NULL;
	self->lock_cache         = lock_cache;
	list_init(&self->readers);
	list_init(&self->readers_wait);
	list_init(&self->writers_wait);
}

hot static inline Lock*
resource_lock(Resource* self, bool shared)
{
	auto coro = am_self();
	bool wait = false;

	Lock* lock = NULL;
	if (shared)
	{
		// reentrant
		list_foreach(&self->readers)
		{
			auto lock = list_at(Lock, link);
			if (lock->coroutine == coro)
			{
				lock->refs++;
				return lock;
			}
		}

		// prepare new lock
		lock = lock_create(self->lock_cache, self, shared);
		if (self->writer == NULL && self->writers_wait_count == 0)
		{
			// no writers, add coroutine to the active list
			self->readers_count++;
			list_append(&self->readers, &lock->link);
		} else
		{
			// writer <- reader
			self->readers_wait_count++;
			list_append(&self->readers_wait, &lock->link);
			wait = true;
		}

	} else
	{
		// exclusive is not reentrant
		assert(!self->writer || (self->writer && self->writer->coroutine != coro));

		// prepare new lock
		lock = lock_create(self->lock_cache, self, shared);
		if (self->writer ||
		    self->writers_wait_count > 0 ||
		    self->readers_count > 0)
		{
			// has writers or active readers, wait
			self->writers_wait_count++;
			list_append(&self->writers_wait, &lock->link);
			wait = true;
		} else
		{
			self->writer = lock;
		}
	}

	if (wait)
	{
		coroutine_suspend(coro);
		assert(! coro->cancel);
	}

	return lock;
}

hot static inline void
resource_unlock(Lock* lock)
{
	assert(lock->coroutine == am_self());
	Resource* self = lock->resource;

	// reentrant
	if (lock->refs > 0)
	{
		lock->refs--;
		return;
	}

	if (lock->shared)
	{
		assert(self->writer == NULL);
		self->readers_count--;
		assert(self->readers_count >= 0);
		list_unlink(&lock->link);
	} else
	{
		assert(self->writer == lock);
		self->writer = NULL;
	}

	// wakeup next writer when no active readers left
	if (self->writers_wait_count > 0)
	{
		if (self->readers_count == 0)
		{
			auto next = list_pop(&self->writers_wait);
			auto writer = container_of(next, Lock, link);
			self->writers_wait_count--;
			assert(self->writers_wait_count >= 0);
			self->writer = writer;
			coroutine_resume(writer->coroutine);
		}

	} else
	{
		// make all pending readers active
		if (self->readers_wait_count > 0)
		{
			self->readers_count     += self->readers_wait_count;
			self->readers_wait_count = 0;
			list_foreach_safe(&self->readers_wait)
			{
				auto reader = list_at(Lock, link);
				list_init(&reader->link);
				list_append(&self->readers, &reader->link);
				coroutine_resume(reader->coroutine);
			}
			list_init(&self->readers_wait);
		}
	}

	lock_cache_push(self->lock_cache, lock);
}

static inline bool
resource_is_locked(Resource* self)
{
	return self->writer                 ||
	       self->writers_wait_count > 0 ||
	       self->readers_count > 0      ||
	       self->readers_wait_count > 0;
}
