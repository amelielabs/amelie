
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

__thread Task* am_task;
__thread void* am_runtime;
__thread void* am_share;

static inline void
task_shutdown(void)
{
	auto coros = &am_task->coroutines;
	if (coros->count == 1)
		return;

	// cancel all coroutines, except the current one
	list_foreach(&coros->list)
	{
		auto coro = list_at(Coroutine, link);
		if (coros->current == coro)
			continue;
		coroutine_cancel(coro);
	}

	// wait for completion
	wait_event(&coros->on_exit, am_self());
}

hot void
task_coroutine_main(void* arg)
{
	auto self = (Coroutine*)arg;

	// main
	auto on_error = error_catch
	(
		cancellation_point();
		self->main(self->main_arg);
	);

	if (unlikely(on_error))
	{
		auto error = &self->error;
		if (error->code != CANCEL) {
			report(error->file, error->function, error->line,
			       "unhandled exception: {buf}",
			        &error->text);
		}
	}

	// main exit
	if (self == am_task->main_coroutine)
	{
		if (on_error)
			cond_signal(&am_task->status, -1);

		// cancel all coroutines, except current one
		task_shutdown();
	}

	// coroutine_join
	event_signal(&self->on_exit);
	self->name[0] = 0;

	auto coros = self->coroutines;
	coroutines_del(coros, self);
	coroutines_add_free(coros, self);

	// wakeup shutdown on last coroutine exit
	if (coros->count == 1)
		event_signal(&coros->on_exit);

	coroutines_switch(coros);

	// unreach
	abort();
}

hot static inline void
mainloop(Task* self)
{
	auto timers     = &self->timers;
	auto poller     = &self->poller;
	auto coroutines = &self->coroutines;
	auto bus        = &self->bus;

	// set initial time
	timers_update(timers);

	// main loop
	for (;;)
	{
		// process pending events and msgs
		bus_step(bus);

		// execute pending coroutines
		coroutines_scheduler(coroutines);

		if (unlikely(! coroutines->count))
			break;

		// retry the loop before blocking wait, if some events are ready
		if (bus_pending(bus) > 0)
			continue;

		// update time
		timers_reset(timers);

		uint32_t timeout = UINT32_MAX;
		if (! timers_empty(timers))
		{
			timers_update(timers);

			// get minimal timer timeout
			auto next = timers_min(timers);
			if (next)
			{
				int diff = next->timeout - timers->time_ms;
				if (diff <= 0)
					timeout = 0;
				else
					timeout = diff;
			}

			// run timers
			timers_step(timers);
		}

		// retry the loop before blocking wait, if some events are ready
		bus_set_sleep(bus, true);
		if (bus_pending(bus) > 0)
		{
			bus_set_sleep(bus, false);
			continue;
		}
		poller_step(poller, timeout);
		bus_set_sleep(bus, false);
	}
}

always_inline static inline void
task_enter(Task* self, void* runtime, void* share)
{
	am_task    = self;
	am_runtime = runtime;
	am_share   = share;
}

hot static void
task_main(Task* self, bool native)
{
	// set global variables
	task_enter(self, self->main_arg_runtime, self->main_arg_share);

	if (! native)
	{
		// get tid
		self->thread.tid = gettid();

		// block signals
		thread_set_sigmask_default();

		// set task name
		thread_set_name(&self->thread, self->name);
	}

	// set random
	random_open(&self->random);

	// create task main coroutine
	Coroutine* main;
	main = coroutines_create(&self->coroutines, task_coroutine_main,
	                         self->main,
	                         self->main_arg);
	coroutine_set_name(main, "{s}", self->name);
	self->main_coroutine = main;

	// schedule coroutines and handle io
	mainloop(self);
}

hot static void*
task_thread_main(void* arg)
{
	Thread* thread = arg;
	Task* self = thread->arg;
	task_main(self, false);
	return NULL;
}

void
task_init(Task* self)
{
	self->main             = NULL;
	self->main_arg         = NULL;
	self->main_arg_runtime = NULL;
	self->main_coroutine   = NULL;
	self->name[0]          = 0;
	buf_cache_init(&self->buf_cache);
	task_log_init(&self->log);
	random_init(&self->random);
	coroutines_init(&self->coroutines, 4096 * 32); // 128kb
	timers_init(&self->timers);
	poller_init(&self->poller);
	bus_init(&self->bus);
	cond_init(&self->status);
	thread_init(&self->thread);
}

void
task_free(Task* self)
{
	coroutines_free(&self->coroutines);
	timers_free(&self->timers);
	bus_close(&self->bus);
	bus_free(&self->bus);
	poller_free(&self->poller);
	cond_free(&self->status);
	task_log_free(&self->log);
	buf_cache_free(&self->buf_cache);
}

bool
task_active(Task* self)
{
	return self->thread.id != 0;
}

int
task_create_nothrow(Task*        self,
                    char*        name,
                    MainFunction main,
                    void*        main_arg,
                    void*        main_arg_runtime,
                    void*        main_arg_share,
                    TaskLogWrite log_write,
                    void*        log_write_arg,
                    Bufs*        bufs)
{
	// set arguments
	self->main             = main;
	self->main_arg         = main_arg;
	self->main_arg_runtime = main_arg_runtime;
	self->main_arg_share   = main_arg_share;
	format(self->name, sizeof(self->name), "{s}", name);

	// prepare buf cache
	int rc = buf_cache_allocate_nothrow(&self->buf_cache, bufs, 1024, 32 * 1024);
	if (unlikely(rc == -1))
		return -1;

	// set logger iface
	task_log_set(&self->log, log_write, log_write_arg);

	// prepare poller
	rc = poller_create(&self->poller);
	if (unlikely(rc == -1))
		return -1;

	// prepare bus
	rc = bus_open(&self->bus, &self->poller);
	if (unlikely(rc == -1))
		return -1;

	// create task thread
	if (main)
	{
		rc = thread_create(&self->thread, task_thread_main, self);
		if (unlikely(rc == -1))
			return -1;
	} else {
		thread_create_self(&self->thread);
	}

	return 0;
}

void
task_execute(Task* self, MainFunction main, void* main_arg)
{
	// save and restore the current task context after execution
	auto _am_share   = am_share;
	auto _am_runtime = am_runtime;
	auto _am_task    = am_task;
	self->main       = main;
	self->main_arg   = main_arg;
	task_main(self, true);
	task_enter(_am_task, _am_runtime, _am_share);
}

void
task_wait(Task* self)
{
	thread_join(&self->thread);
}
