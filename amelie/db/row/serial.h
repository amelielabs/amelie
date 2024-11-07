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

typedef struct Serial Serial;

struct Serial
{
	atomic_u64 value;
	Spinlock   lock;
};

static inline void
serial_init(Serial* self)
{
	self->value = 0;
	spinlock_init(&self->lock);
}

static inline void
serial_free(Serial* self)
{
	spinlock_free(&self->lock);
}

static inline void
serial_set(Serial* self, uint64_t value)
{
	atomic_u64_set(&self->value, value);
}

static inline uint64_t
serial_next(Serial* self)
{
	return atomic_u64_inc(&self->value);
}

static inline uint64_t
serial_get(Serial* self)
{
	return atomic_u64_of(&self->value);
}

static inline void
serial_sync(Serial* self, uint64_t value)
{
	spinlock_lock(&self->lock);
	auto next = value + 1;
	if (self->value < next)
		self->value = next;
	spinlock_unlock(&self->lock);
}
