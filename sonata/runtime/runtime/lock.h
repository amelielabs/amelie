#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Lock Lock;

struct Lock
{
	int          readers_count;
	List         readers;
	int          readers_wait_count;
	List         readers_wait;
	Locker*      writer;
	int          writers_wait_count;
	List         writers_wait;
	LockerCache* locker_cache;
};

static inline void
lock_init(Lock* self, LockerCache* locker_cache)
{
	self->readers_count      = 0;
	self->readers_wait_count = 0;
	self->writers_wait_count = 0;
	self->writer             = NULL;
	self->locker_cache       = locker_cache;
	list_init(&self->readers);
	list_init(&self->readers_wait);
	list_init(&self->writers_wait);
}

hot static inline Locker*
lock_lock(Lock* self, bool shared)
{
	auto coro = so_self();
	bool wait = false;

	Locker* locker = NULL;
	if (shared)
	{
		// reentrant
		list_foreach(&self->readers)
		{
			auto locker = list_at(Locker, link);
			if (locker->coroutine == coro)
			{
				locker->refs++;
				return locker;
			}
		}

		// prepare new locker
		locker = locker_create(self->locker_cache, self, shared);

		if (self->writer == NULL && self->writers_wait_count == 0)
		{
			// no writers, add coroutine to the active list
			self->readers_count++;
			list_append(&self->readers, &locker->link);
		} else
		{
			// writer <- reader
			self->readers_wait_count++;
			list_append(&self->readers_wait, &locker->link);
			wait = true;
		}

	} else
	{
		// exclusive is not reentrant
		assert(!self->writer || (self->writer && self->writer->coroutine != coro));

		// prepare new locker
		locker = locker_create(self->locker_cache, self, shared);

		if (self->writer ||
		    self->writers_wait_count > 0 ||
		    self->readers_count > 0)
		{
			// has writers or active readers, wait
			self->writers_wait_count++;
			list_append(&self->writers_wait, &locker->link);
			wait = true;
		} else
		{
			self->writer = locker;
		}
	}

	if (wait)
	{
		coroutine_suspend(coro);
		assert(! coro->cancel);
	}

	return locker;
}

hot static inline void
lock_unlock(Locker* locker)
{
	assert(locker->coroutine == so_self());
	Lock* self = locker->lock;

	// reentrant
	if (locker->refs > 0)
	{
		locker->refs--;
		return;
	}

	if (locker->shared)
	{
		assert(self->writer == NULL);
		self->readers_count--;
		assert(self->readers_count >= 0);
		list_unlink(&locker->link);
	} else
	{
		assert(self->writer == locker);
		self->writer = NULL;
	}

	// wakeup next writer when no active readers left
	if (self->writers_wait_count > 0)
	{
		if (self->readers_count == 0)
		{
			auto next = list_pop(&self->writers_wait);
			auto writer = container_of(next, Locker, link);
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
				auto reader = list_at(Locker, link);
				list_init(&reader->link);
				list_append(&self->readers, &reader->link);
				coroutine_resume(reader->coroutine);
			}
			list_init(&self->readers_wait);
		}
	}

	locker_cache_push(self->locker_cache, locker);
}

static inline bool
lock_is_locked(Lock* self)
{
	return self->writer                 ||
	       self->writers_wait_count > 0 ||
	       self->readers_count > 0      ||
	       self->readers_wait_count > 0;
}
