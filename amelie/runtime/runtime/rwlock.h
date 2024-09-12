#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Rwlock Rwlock;

struct Rwlock
{
	pthread_rwlock_t lock;
};

static inline void
rwlock_init(Rwlock* self)
{
	pthread_rwlock_init(&self->lock, NULL);
}

static inline void
rwlock_free(Rwlock* self)
{
	pthread_rwlock_destroy(&self->lock);
}

static inline void
rwlock_rdlock(Rwlock* self)
{
	pthread_rwlock_rdlock(&self->lock);
}

static inline void
rwlock_wrlock(Rwlock* self)
{
	pthread_rwlock_wrlock(&self->lock);
}

static inline void
rwlock_lock(Rwlock* self, bool shared)
{
	if (shared)
		rwlock_rdlock(self);
	else
		rwlock_wrlock(self);
}

static inline void
rwlock_unlock(Rwlock* self)
{
	pthread_rwlock_unlock(&self->lock);
}
