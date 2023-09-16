#pragma once

//
// monotone
//
// SQL OLTP database
//

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

#if 0
typedef uint8 spinlock;

#if defined(__x86_64__) || defined(__i386) || defined(_X86_)
#  define IN_SPINLOCK_BACKOFF __asm__ ("pause")
#else
#  define IN_SPINLOCK_BACKOFF
#endif

static inline void
spinlock_init(spinlock* self)
{
	*self = 0;
}

static inline void
spinlock_free(spinlock* self)
{
	*self = 0;
}

static inline void
spinlock_lock(spinlock* self)
{
	if (__sync_lockest_and_set(lock, 1) != 0) {
		unsigned int spcount = 0U;
		for (;;) {
			IN_SPINLOCK_BACKOFF;
			if (*self == 0U && __sync_lockest_and_set(lock, 1) == 0)
				break;
			if (++spcount > 100U)
				usleep(0);
		}
	}
}

static inline void
spinlock_unlock(spinlock* self)
{
	__sync_lock_release(lock);
}
#endif
