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

typedef struct Clock Clock;

struct Clock
{
	uint64_t time_ms;
	uint64_t time_us;
	uint64_t time_ns;
	bool     time_cached;
};

static inline uint64_t
clock_now(void)
{
	struct timespec t;
	clock_gettime(CLOCK_REALTIME_COARSE, &t);
	return t.tv_sec * (uint64_t)1e9 + t.tv_nsec;
}

static inline void
clock_init(Clock* self)
{
	self->time_ms     = 0;
	self->time_ns     = 0;
	self->time_us     = 0;
	self->time_cached = false;
}

static inline void
clock_update(Clock* self)
{
	if (self->time_cached)
		return;
	self->time_ns     = clock_now();
	self->time_ms     = self->time_ns / 1000000;
	self->time_us     = self->time_ns / 1000;
	self->time_cached = true;
}

static inline void
clock_reset(Clock* self)
{
	self->time_cached = false;
}

static inline uint64_t
clock_time_ns(Clock* self)
{
	clock_update(self);
	return self->time_ns;
}

static inline uint64_t
clock_time_ms(Clock* self)
{
	clock_update(self);
	return self->time_ms;
}

static inline uint64_t
clock_time_us(Clock* self)
{
	clock_update(self);
	return self->time_us;
}

static inline void
time_start(uint64_t* time_us)
{
	*time_us = clock_now();
}

static inline void
time_end(uint64_t* time_us)
{
	*time_us = (clock_now() - *time_us) / 1000;
}
