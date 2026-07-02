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

typedef struct Timer  Timer;
typedef struct Timers Timers;

typedef void (*TimerFunction)(Timer*);

struct Timer
{
	bool          active;
	uint64_t      seq;
	uint64_t      timeout;
	int           interval;
	TimerFunction function;
	void*         function_arg;
	void*         timers;
};

struct Timers
{
	uint64_t time_ms;
	uint64_t time_us;
	uint64_t time_ns;
	bool     time_cached;
	Timer**  timers;
	int      timers_count;
	int      timers_max;
	uint64_t timers_seq;
};

void     timers_init(Timers*);
void     timers_free(Timers*);
void     timers_update(Timers*);
int      timers_step(Timers*);
void     timers_add(Timers*, Timer*);
void     timers_remove(Timers*, Timer*);
uint64_t timers_gettime(void);

always_inline static inline bool
timers_empty(Timers* self)
{
	return !self->timers_count;
}

always_inline static inline void
timers_reset(Timers* self)
{
	self->time_cached = false;
}

static inline Timer*
timers_min(Timers* self)
{
	if (self->timers_count == 0)
		return NULL;
	return self->timers[0];
}

static inline uint64_t
timers_time_ns(Timers* self)
{
	timers_update(self);
	return self->time_ns;
}

static inline uint64_t
timers_time_ms(Timers* self)
{
	timers_update(self);
	return self->time_ms;
}

static inline uint64_t
timers_time_us(Timers* self)
{
	timers_update(self);
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
	self->timers       = NULL;
}

static inline void
timer_start(Timers* self, Timer* timer)
{
	timers_update(self);
	timers_add(self, timer);
}

static inline void
timer_stop(Timer* self)
{
	if (! self->active)
		return;
	timers_remove(self->timers, self);
}

static inline void
time_start(uint64_t* time_us)
{
	*time_us = timers_gettime();
}

static inline void
time_end(uint64_t* time_us)
{
	*time_us = (timers_gettime() - *time_us) / 1000;
}
