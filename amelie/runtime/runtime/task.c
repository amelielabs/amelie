
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>

__thread Task*      am_self;
__thread atomic_u32 am_cancelled = 0;

static void
task_on_sigusr2(int signal)
{
	atomic_u32_set(&am_cancelled, 1);
	unused(signal);
}

hot static void*
task_main(void* arg)
{
	Thread* thread = arg;
	Task* self = thread->arg;

	// set global variable
	am_self = self;

	// block signals
	thread_set_sigmask_default();

	// set signal used for cancellation, to interrupt syscalls
	sigset_t mask;
	sigemptyset(&mask);
	sigaddset(&mask, SIGUSR2);
	pthread_sigmask(SIG_UNBLOCK, &mask, NULL);

	struct sigaction act;
	memset(&act, 0, sizeof(act));
	act.sa_handler = task_on_sigusr2;
	sigaction(SIGUSR2, &act, NULL);

	// set task name
	thread_update_name(&self->thread);

	// set initial time
	clock_update(&self->clock);

	// main
	Exception e;
	if (enter(&e))
	{
		self->main(self->main_arg);
	}

	if (unlikely(leave(&e)))
	{
		auto error = &self->error;
		if (error->code != CANCEL) {
			report(error->file, error->function, error->line,
			       "error: ",
			       "unhandled exception: %s", error->text);
		}
	}

	if (self->on_exit)
		self->on_exit(self->on_exit_arg);
	return NULL;
}

void
task_init(Task* self)
{
	self->main            = NULL;
	self->main_arg        = NULL;
	self->main_arg_global = NULL;
	self->on_exit         = NULL;
	self->on_exit_arg     = NULL;
	self->log_write       = NULL;
	self->log_write_arg   = NULL;
	exception_mgr_init(&self->exception_mgr);
	error_init(&self->error);
	channel_init(&self->channel);
	clock_init(&self->clock);
	poller_init(&self->poller);
	arena_init(&self->arena, 4000);
	buf_list_init(&self->buf_list);
	buf_cache_init(&self->buf_cache, 32 * 1024); // 32kb max
	cond_init(&self->status);
	thread_init(&self->thread);
}

void
task_free(Task* self)
{
	arena_reset(&self->arena);
	channel_free(&self->channel);
	buf_list_free(&self->buf_list);
	buf_cache_free(&self->buf_cache);
	poller_free(&self->poller);
	cond_free(&self->status);
}

bool
task_created(Task* self)
{
	return thread_created(&self->thread);
}

void
task_set_on_exit(Task* self, TaskFunction func, void* func_arg)
{
	self->on_exit     = func;
	self->on_exit_arg = func_arg;
}

int
task_create_nothrow(Task*        self,
                    char*        name,
                    TaskFunction main,
                    void*        main_arg,
                    void*        main_arg_global,
                    LogFunction  log,
                    void*        log_arg)
{
	// set arguments
	self->main            = main;
	self->main_arg        = main_arg;
	self->main_arg_global = main_arg_global;
	self->log_write       = log;
	self->log_write_arg   = log_arg;

	// prepare poller
	int rc = poller_create(&self->poller);
	if (unlikely(rc == -1))
		return -1;

	// set task name
	thread_set_name(&self->thread, name);

	// create task thread
	rc = thread_create(&self->thread, task_main, self);
	if (unlikely(rc == -1))
		return -1;

	return 0;
}

void
task_detach(Task* self)
{
	thread_detach(&self->thread);
}

void
task_cancel(Task* self)
{
	thread_kill(&self->thread, SIGUSR2);
}

void
task_wait(Task* self)
{
	thread_join(&self->thread);
}
