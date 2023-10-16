#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ReqLock ReqLock;

struct ReqLock
{
	Spinlock lock;
};

static inline void
req_lock_init(ReqLock* self)
{
	spinlock_init(&self->lock);
}

static inline void
req_lock_free(ReqLock* self)
{
	spinlock_free(&self->lock);
}

static inline void
req_lock(ReqLock* self)
{
	spinlock_lock(&self->lock);
}

static inline void
req_unlock(ReqLock* self)
{
	spinlock_unlock(&self->lock);
}
