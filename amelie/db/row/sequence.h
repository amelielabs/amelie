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

typedef struct Sequence Sequence;

struct Sequence
{
	atomic_u64 value;
	Spinlock   lock;
};

static inline void
sequence_init(Sequence* self)
{
	self->value = 0;
	spinlock_init(&self->lock);
}

static inline void
sequence_free(Sequence* self)
{
	spinlock_free(&self->lock);
}

static inline void
sequence_set(Sequence* self, uint64_t value)
{
	atomic_u64_set(&self->value, value);
}

static inline uint64_t
sequence_next(Sequence* self)
{
	return atomic_u64_inc(&self->value);
}

static inline uint64_t
sequence_get(Sequence* self)
{
	return atomic_u64_of(&self->value);
}

static inline void
sequence_sync(Sequence* self, uint64_t value)
{
	spinlock_lock(&self->lock);
	auto next = value + 1;
	if (self->value < next)
		self->value = next;
	spinlock_unlock(&self->lock);
}
