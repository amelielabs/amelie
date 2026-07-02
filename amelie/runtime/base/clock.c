
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
clock_init(Clock* self)
{
	self->time_ms      = 0;
	self->time_ns      = 0;
	self->time_us      = 0;
	self->time_cached  = false;
	self->clock_count = 0;
	self->clock_max   = 16;
	self->clock_seq   = 0;
	self->clock       = NULL;
}

void
clock_free(Clock* self)
{
	if (self->clock)
		am_free(self->clock);
}

uint64_t
clock_time(void)
{
	struct timespec t;
	clock_gettime(CLOCK_REALTIME_COARSE, &t);
	return t.tv_sec * (uint64_t)1e9 + t.tv_nsec;
}

void
clock_update(Clock* self)
{
	if (self->time_cached)
		return;
	self->time_ns     = clock_time();
	self->time_ms     = self->time_ns / 1000000;
	self->time_us     = self->time_ns / 1000;
	self->time_cached = true;
}

hot int
clock_step(Clock* self)
{
	if (self->clock_count == 0)
		return 0;

	int clock_hit = 0;
	int i = 0;
	for (; i < self->clock_count; i++)
	{
		Timer* timer = self->clock[i];
		if (timer->timeout > self->time_ms)
			break;
		timer->function(timer);
		timer->active = false;
		clock_hit++;
	}
	if (! clock_hit)
		return 0;
	int clock_left = self->clock_count - clock_hit;
	if (clock_left == 0)
	{
		self->clock_count = 0;
		return clock_hit;
	}

	memmove(self->clock, self->clock + clock_hit, sizeof(Timer*) * clock_left);
	self->clock_count -= clock_hit;
	return clock_hit;
}

hot static int
clock_cmp(const void* a_ptr, const void* b_ptr)
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
clock_add(Clock* self, Timer* timer)
{
	int count = self->clock_count + 1;
	if (self->clock == NULL || count >= self->clock_max)
	{
		int   clock_max = self->clock_max * 2;
		void* clock = am_realloc(self->clock, clock_max * sizeof(Timer*));
		self->clock = clock;
		self->clock_max = clock_max;
	}

	self->clock[count - 1] = timer;
	timer->seq     = self->clock_seq++;
	timer->timeout = self->time_ms + timer->interval;
	timer->active  = true;
	timer->clock  = self;
	qsort(self->clock, count, sizeof(Timer*), clock_cmp);
	self->clock_count = count;
}

hot void
clock_remove(Clock* self, Timer* timer)
{
	if (! timer->active)
		return;
	assert(self->clock_count >= 1);
	int i = 0;
	int j = 0;
	for (; i < self->clock_count; i++)
	{
		if (self->clock[i] == timer)
			continue;
		self->clock[j] = self->clock[i];
		j++;
	}
	timer->active = 0;
	self->clock_count -= 1;
}
