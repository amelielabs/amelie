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

hot static inline void
event_signal_direct(Event* event)
{
	event->signal = true;
	if (event->parent)
	{
		Event* parent = event->parent;
		if (parent->parent_signal == NULL)
			parent->parent_signal = event;
		event_signal_direct(event->parent);
	}
	if (event->wait)
		coroutine_resume(event->wait);
}

static inline void
wait_event_timer(Timer* timer)
{
	Event* event = timer->function_arg;
	coroutine_resume(event->wait);
}

hot static inline bool
wait_event(Event* event, TimerMgr* timer_mgr, Coroutine* coro, int time_ms)
{
	if (event->signal)
	{
		event->signal = false;
		return false;
	}

	Timer timer;
	timer_init(&timer, wait_event_timer, event, time_ms);
	if (time_ms >= 0)
		timer_start(timer_mgr, &timer);

	assert(event->wait == NULL);
	event->wait = coro;

	// wait for signal
	coroutine_suspend(coro);
	event->wait = NULL;

	bool timedout = time_ms > 0 && !timer.active;
	if (unlikely(timer.active))
		timer_stop(&timer);

	event->signal = false;
	return timedout;
}
