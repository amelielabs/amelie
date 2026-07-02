
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
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_rel.h>
#include <amelie_runtime.h>

void
runtime_init(Runtime* self)
{
	self->timezone = NULL;
	self->crc      = crc32_sse_supported() ? crc32_sse : crc32;
	self->iface    = NULL;
	bufs_init(&self->bufs);
	config_init(&self->config);
	state_init(&self->state);
	timezones_init(&self->timezones);
	codec_cache_init(&self->cache_compression);
	jobs_init(&self->jobs);
	logger_init(&self->logger);
	task_init(&self->task);
	locks_init(&self->locks);
	lockables_init(&self->lockables);
}

void
runtime_free(Runtime* self)
{
	locks_free(&self->locks);
	lockables_free(&self->lockables);
	task_free(&self->task);
	jobs_free(&self->jobs);
	config_free(&self->config);
	state_free(&self->state);
	timezones_free(&self->timezones);
	codec_cache_free(&self->cache_compression);
	bufs_free(&self->bufs);
	logger_close(&self->logger);
	tls_lib_free();
}

typedef struct
{
	RuntimeMain main;
	void*       main_arg;
	int         argc;
	char**      argv;
} RuntimeArgs;

static void
runtime_prepare(Runtime* self)
{
	// prepare default logger settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_stdout(logger, true);
	logger_set_stdout_time(logger, false);
	logger_set_stdout_lf(logger, true);

	// init ssl library
	tls_lib_init();

	// set UTC time by default (for posix time api)
	setenv("TZ", "UTC", true);

	// read time zones
	timezones_open(&self->timezones);

	// set default timezone as system timezone
	self->timezone = self->timezones.system;
	logger_set_timezone(logger, self->timezone);

	// start background job manager
	jobs_start(&self->jobs, 1);

	// prepare default configuration
	config_prepare(&self->config);

	// prepare state
	state_prepare(&self->state);
}

static void
runtime_shutdown(Runtime* self)
{
	// stop background job manager
	jobs_stop(&self->jobs);
}

static void
runtime_main(void* arg)
{
	RuntimeArgs* args = arg;
	auto self = runtime();
	auto on_error = error_catch
	(
		runtime_prepare(self);
		args->main(args->main_arg, args->argc, args->argv);
	);
	runtime_shutdown(self);

	// notify for completion
	RuntimeStatus status = RUNTIME_COMPLETE;
	if (on_error)
		status = RUNTIME_ERROR;
	cond_signal(&self->task.status, status);
}

RuntimeStatus
runtime_start(Runtime* self, RuntimeMain main, void* main_arg, int argc, char** argv)
{
	RuntimeArgs args =
	{
		.main     = main,
		.main_arg = main_arg,
		.argc     = argc,
		.argv     = argv,
	};
	int rc;
	rc = bufs_allocate_nothrow(&self->bufs, 64000);
	if (unlikely(rc == -1))
		return RUNTIME_ERROR;

	rc = task_create_nothrow(&self->task, "main", runtime_main, &args, self, NULL,
	                         logger_write, &self->logger,
	                         &self->bufs);
	if (unlikely(rc == -1))
		return RUNTIME_ERROR;
	rc = cond_wait(&self->task.status);
	return rc;
}

void
runtime_stop(Runtime* self)
{
	if (task_active(&self->task))
	{
		Msg stop;
		msg_init(&stop, MSG_STOP);
		task_send(&self->task, &stop);
		task_wait(&self->task);
	}
}
