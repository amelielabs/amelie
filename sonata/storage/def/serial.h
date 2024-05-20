#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Serial Serial;

struct Serial
{
	atomic_u64 value;
};

static inline void
serial_init(Serial* self)
{
	self->value = 0;
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
