#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Clock Clock;

struct Clock
{
	uint64_t time_ms;
	uint64_t time_us;
	uint64_t time_ns;
};

static inline void
clock_init(Clock* self)
{
	self->time_ms = 0;
	self->time_us = 0;
	self->time_ns = 0;
}

static inline uint64_t
clock_ns(void)
{
	struct timespec t;
	clock_gettime(CLOCK_REALTIME_COARSE, &t);
	return t.tv_sec * (uint64_t)1e9 + t.tv_nsec;
}

static inline uint64_t
clock_us(void)
{
	return clock_ns() / 1000;
}

static inline void
clock_update(Clock* self)
{
	self->time_ns = clock_ns();
	self->time_us = self->time_ns / 1000;
	self->time_ms = self->time_ns / 1000000;
}

static inline void
time_start(uint64_t* time_us)
{
	*time_us = clock_us();
}

static inline void
time_end(uint64_t* time_us)
{
	*time_us = clock_us() - *time_us;
}
