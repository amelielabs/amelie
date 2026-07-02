
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

#include <amelie_base.h>

void
timers_init(Timers* self)
{
	self->time_ms      = 0;
	self->time_ns      = 0;
	self->time_us      = 0;
	self->time_cached  = false;
	self->timers_count = 0;
	self->timers_max   = 16;
	self->timers_seq   = 0;
	self->timers       = NULL;
}

void
timers_free(Timers* self)
{
	if (self->timers)
		am_free(self->timers);
}

uint64_t
timers_gettime(void)
{
	struct timespec t;
	clock_gettime(CLOCK_REALTIME_COARSE, &t);
	return t.tv_sec * (uint64_t)1e9 + t.tv_nsec;
}

void
timers_update(Timers* self)
{
	if (self->time_cached)
		return;
	self->time_ns     = timers_gettime();
	self->time_ms     = self->time_ns / 1000000;
	self->time_us     = self->time_ns / 1000;
	self->time_cached = true;
}

hot int
timers_step(Timers* self)
{
	if (self->timers_count == 0)
		return 0;

	int timers_hit = 0;
	int i = 0;
	for (; i < self->timers_count; i++)
	{
		Timer* timer = self->timers[i];
		if (timer->timeout > self->time_ms)
			break;
		timer->function(timer);
		timer->active = false;
		timers_hit++;
	}
	if (! timers_hit)
		return 0;
	int timers_left = self->timers_count - timers_hit;
	if (timers_left == 0)
	{
		self->timers_count = 0;
		return timers_hit;
	}

	memmove(self->timers, self->timers + timers_hit, sizeof(Timer*) * timers_left);
	self->timers_count -= timers_hit;
	return timers_hit;
}

hot static int
timers_cmp(const void* a_ptr, const void* b_ptr)
{
	auto a = *(Timer**)a_ptr;
	auto b = *(Timer**)b_ptr;

	if (a->timeout == b->timeout)
		return (a->seq > b->seq) ? 1 : -1;

	if (a->timeout > b->timeout)
		return 1;

	return -1;
}

hot void
timers_add(Timers* self, Timer* timer)
{
	int count = self->timers_count + 1;
	if (self->timers == NULL || count >= self->timers_max)
	{
		int   timers_max = self->timers_max * 2;
		void* timers = am_realloc(self->timers, timers_max * sizeof(Timer*));
		self->timers = timers;
		self->timers_max = timers_max;
	}

	self->timers[count - 1] = timer;
	timer->seq     = self->timers_seq++;
	timer->timeout = self->time_ms + timer->interval;
	timer->active  = true;
	timer->timers  = self;
	qsort(self->timers, count, sizeof(Timer*), timers_cmp);
	self->timers_count = count;
}

hot void
timers_remove(Timers* self, Timer* timer)
{
	if (! timer->active)
		return;
	assert(self->timers_count >= 1);
	int i = 0;
	int j = 0;
	for (; i < self->timers_count; i++)
	{
		if (self->timers[i] == timer)
			continue;
		self->timers[j] = self->timers[i];
		j++;
	}
	timer->active = 0;
	self->timers_count -= 1;
}
