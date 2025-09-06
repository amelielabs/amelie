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

typedef uint64_t Spinlock;

static inline void
spinlock_init(Spinlock* self)
{
	*self = 0;
}

static inline void
spinlock_free(Spinlock* self)
{
	*self = 0;
}

hot static inline void
spinlock_lock(Spinlock* self)
{
	if (__sync_lock_test_and_set(self, 1) != 0)
	{
		unsigned int count = 0;
		for (;;) {
			__asm__("pause");
			if (*self == 0 && __sync_lock_test_and_set(self, 1) == 0)
				break;
			if (++count > 300)
				sched_yield();
		}
	}
}

static inline void
spinlock_unlock(Spinlock* self)
{
	__sync_lock_release(self);
}

#if 0
typedef struct Spinlock Spinlock;

struct Spinlock
{
	uint64_t state;
	char     pad[cache_line - sizeof(uint64_t)];
};

static inline void
spinlock_init(Spinlock* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
spinlock_free(Spinlock* self)
{
	memset(self, 0, sizeof(*self));
}

hot static inline void
spinlock_lock(Spinlock* self)
{
	if (__sync_lock_test_and_set(&self->state, 1) != 0)
	{
		unsigned int count = 0;
		for (;;) {
			__asm__("pause");
			if (self->state == 0 && __sync_lock_test_and_set(&self->state, 1) == 0)
				break;
			if (++count > 100)
				usleep(0);
		}
	}
}

static inline void
spinlock_unlock(Spinlock* self)
{
	__sync_lock_release(&self->state);
}
#endif

#if 0
typedef struct Spinlock Spinlock;

struct Spinlock
{
	pthread_spinlock_t lock;
};

static inline void
spinlock_init(Spinlock* self)
{
	pthread_spin_init(&self->lock, PTHREAD_PROCESS_PRIVATE);
}

static inline void
spinlock_free(Spinlock* self)
{
	pthread_spin_destroy(&self->lock);
}

static inline void
spinlock_lock(Spinlock* self)
{
	pthread_spin_lock(&self->lock);
}

static inline void
spinlock_unlock(Spinlock* self)
{
	pthread_spin_unlock(&self->lock);
}
#endif
