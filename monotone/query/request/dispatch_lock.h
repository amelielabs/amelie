#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct DispatchLock DispatchLock;

struct DispatchLock
{
	Spinlock lock;
};

static inline void
dispatch_lock_init(DispatchLock* self)
{
	spinlock_init(&self->lock);
}

static inline void
dispatch_lock_free(DispatchLock* self)
{
	spinlock_free(&self->lock);
}

static inline void
dispatch_lock(DispatchLock* self)
{
	spinlock_lock(&self->lock);
}

static inline void
dispatch_unlock(DispatchLock* self)
{
	spinlock_unlock(&self->lock);
}
