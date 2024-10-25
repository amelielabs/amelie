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

typedef struct Timer    Timer;
typedef struct TimerMgr TimerMgr;

typedef void (*TimerFunction)(Timer*);

struct Timer
{
	bool          active;
	uint64_t      seq;
	uint64_t      timeout;
	int           interval;
	TimerFunction function;
	void*         function_arg;
	void*         mgr;
};

struct TimerMgr
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

void     timer_mgr_init(TimerMgr*);
void     timer_mgr_free(TimerMgr*);
void     timer_mgr_update(TimerMgr*);
int      timer_mgr_step(TimerMgr*);
void     timer_mgr_add(TimerMgr*, Timer*);
void     timer_mgr_remove(TimerMgr*, Timer*);
uint64_t timer_mgr_gettime(void);

static inline void
timer_mgr_reset(TimerMgr* self)
{
	self->time_cached = false;
}

static inline uint64_t
timer_mgr_time_ns(TimerMgr* self)
{
	timer_mgr_update(self);
	return self->time_ns;
}

static inline uint64_t
timer_mgr_time_ms(TimerMgr* self)
{
	timer_mgr_update(self);
	return self->time_ms;
}

static inline uint64_t
timer_mgr_time_us(TimerMgr* self)
{
	timer_mgr_update(self);
	return self->time_us;
}

static inline bool
timer_mgr_has_active_timers(TimerMgr* self)
{
	return self->timers_count > 0;
}

static inline Timer*
timer_mgr_timer_min(TimerMgr* self)
{
	if (self->timers_count == 0)
		return NULL;
	return self->timers[0];
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
	self->mgr          = NULL;
}

static inline void
timer_start(TimerMgr* self, Timer* timer)
{
	timer_mgr_update(self);
	timer_mgr_add(self, timer);
}

static inline void
timer_stop(Timer* self)
{
	if (! self->active)
		return;
	timer_mgr_remove(self->mgr, self);
}

static inline void
time_start(uint64_t* time_us)
{
	*time_us = timer_mgr_gettime();
}

static inline void
time_end(uint64_t* time_us)
{
	*time_us = (timer_mgr_gettime() - *time_us) / 1000;
}
