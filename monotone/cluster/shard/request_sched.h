#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestSched RequestSched;

struct RequestSched
{
	Spinlock lock;
};

static inline void
request_sched_init(RequestSched* self)
{
	spinlock_init(&self->lock);
}

static inline void
request_sched_free(RequestSched* self)
{
	spinlock_free(&self->lock);
}

static inline void
request_sched_lock(RequestSched* self)
{
	spinlock_lock(&self->lock);
}

static inline void
request_sched_unlock(RequestSched* self)
{
	spinlock_unlock(&self->lock);
}
