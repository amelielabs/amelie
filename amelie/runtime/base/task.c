
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
task_shutdown(CoroutineMgr* mgr)
{
	if (mgr->count == 1)
		return;

	// cancel all coroutines, except the current one
	list_foreach(&mgr->list) {
		auto coro = list_at(Coroutine, link);
		if (mgr->current == coro)
			continue;
		coroutine_cancel(coro);
	}

	// wait for completion
	wait_event(&mgr->on_exit, am_self());
}

hot void
task_coroutine_main(void* arg)
{
	Coroutine* coro = arg;
	CoroutineMgr* mgr = coro->mgr;

	// main
	auto on_error = error_catch
	(
		cancellation_point();
		coro->main(coro->main_arg);
	);

	if (unlikely(on_error))
	{
		auto error = &coro->error;
		if (error->code != CANCEL) {
			report(error->file, error->function, error->line,
			       "error: ",
			       "unhandled exception: %s", error->text);
		}
	}

	// main exit
	if (coro == am_task->main_coroutine)
	{
		if (on_error)
			cond_signal(&am_task->status, -1);

		// cancel all coroutines, except current one
		task_shutdown(mgr);
	}

	// coroutine_join
	event_signal(&coro->on_exit);
	coro->name[0] = 0;

	coroutine_mgr_del(mgr, coro);
	coroutine_mgr_add_free(mgr, coro);

	// wakeup shutdown on last coroutine exit
	if (mgr->count == 1)
		event_signal(&mgr->on_exit);

	coroutine_mgr_switch(mgr);

	// unreach
	abort();
}

hot static inline void
mainloop(Task* self)
{
	auto timer_mgr = &self->timer_mgr;
	auto poller = &self->poller;
	auto coroutine_mgr = &self->coroutine_mgr;
	auto bus = &self->bus;

	// set initial time
	timer_mgr_update(timer_mgr);

	// main loop
	for (;;)
	{
		// process pending events and msgs
		bus_step(bus);

		// execute pending coroutines
		coroutine_mgr_scheduler(coroutine_mgr);

		if (unlikely(! coroutine_mgr->count))
			break;

		// retry the loop before blocking wait, if some events are ready
		if (bus_pending(bus) > 0)
			continue;

		// update timer_mgr time
		timer_mgr_reset(timer_mgr);

		uint32_t timeout = UINT32_MAX;
		if (timer_mgr_has_timers(timer_mgr))
		{
			timer_mgr_update(timer_mgr);

			// get minimal timer timeout
			Timer* next;
			next = timer_mgr_timer_min(timer_mgr);
			if (next)
			{
				int diff = next->timeout - timer_mgr->time_ms;
				if (diff <= 0)
					timeout = 0;
				else
					timeout = diff;
			}

			// run timers
			timer_mgr_step(timer_mgr);
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

	// create task main coroutine
	Coroutine* main;
	main = coroutine_mgr_create(&self->coroutine_mgr, task_coroutine_main,
	                            self->main,
	                            self->main_arg);
	coroutine_set_name(main, "%s", self->name);
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
	self->log_write        = NULL;
	self->log_write_arg    = NULL;
	self->name[0]          = 0;
	coroutine_mgr_init(&self->coroutine_mgr, 4096 * 32); // 128kb
	timer_mgr_init(&self->timer_mgr);
	poller_init(&self->poller);
	bus_init(&self->bus);
	cond_init(&self->status);
	thread_init(&self->thread);
}

void
task_free(Task* self)
{
	coroutine_mgr_free(&self->coroutine_mgr);
	timer_mgr_free(&self->timer_mgr);
	bus_close(&self->bus);
	bus_free(&self->bus);
	poller_free(&self->poller);
	cond_free(&self->status);
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
                    LogFunction  log,
                    void*        log_arg,
                    BufMgr*      buf_mgr)
{
	// set arguments
	self->main             = main;
	self->main_arg         = main_arg;
	self->main_arg_runtime = main_arg_runtime;
	self->main_arg_share   = main_arg_share;
	self->log_write        = log;
	self->log_write_arg    = log_arg;
	self->buf_mgr          = buf_mgr;
	snprintf(self->name, sizeof(self->name), "%s", name);

	// prepare poller
	int rc = poller_create(&self->poller);
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
