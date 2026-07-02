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

typedef struct Timer Timer;
typedef struct Clock Clock;

typedef void (*TimerFunction)(Timer*);

struct Timer
{
	bool          active;
	uint64_t      seq;
	uint64_t      timeout;
	int           interval;
	TimerFunction function;
	void*         function_arg;
	Clock*        clock;
};

struct Clock
{
	uint64_t time_ms;
	uint64_t time_us;
	uint64_t time_ns;
	bool     time_cached;
	Timer**  clock;
	int      clock_count;
	int      clock_max;
	uint64_t clock_seq;
};

void     clock_init(Clock*);
void     clock_free(Clock*);
void     clock_update(Clock*);
int      clock_step(Clock*);
void     clock_add(Clock*, Timer*);
void     clock_remove(Clock*, Timer*);
uint64_t clock_time(void);

always_inline static inline bool
clock_empty(Clock* self)
{
	return !self->clock_count;
}

always_inline static inline void
clock_reset(Clock* self)
{
	self->time_cached = false;
}

static inline Timer*
clock_min(Clock* self)
{
	if (self->clock_count == 0)
		return NULL;
	return self->clock[0];
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
timer_init(Timer* self, TimerFunction function, void* arg,
           int interval)
{
	self->active       = false;
	self->seq          = 0;
	self->timeout      = 0;
	self->interval     = interval;
	self->function     = function;
	self->function_arg = arg;
	self->clock        = NULL;
}

static inline void
timer_start(Clock* self, Timer* timer)
{
	clock_update(self);
	clock_add(self, timer);
}

static inline void
timer_stop(Timer* self)
{
	if (! self->active)
		return;
	clock_remove(self->clock, self);
}

static inline void
time_start(uint64_t* time_us)
{
	*time_us = clock_time();
}

static inline void
time_end(uint64_t* time_us)
{
	*time_us = (clock_time() - *time_us) / 1000;
}
