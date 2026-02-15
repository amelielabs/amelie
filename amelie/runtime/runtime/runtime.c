
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
#include <amelie_json.h>
#include <amelie_runtime.h>

void
runtime_init(Runtime* self)
{
	self->timezone = NULL;
	self->crc      = crc32_sse_supported() ? crc32_sse : crc32;
	self->iface    = NULL;
	buf_mgr_init(&self->buf_mgr);
	config_init(&self->config);
	state_init(&self->state);
	timezone_mgr_init(&self->timezone_mgr);
	random_init(&self->random);
	codec_cache_init(&self->cache_compression);
	job_mgr_init(&self->job_mgr);
	logger_init(&self->logger);
	task_init(&self->task);
	lock_mgr_init(&self->lock_mgr);
}

void
runtime_free(Runtime* self)
{
	lock_mgr_free(&self->lock_mgr);
	task_free(&self->task);
	job_mgr_free(&self->job_mgr);
	config_free(&self->config);
	state_free(&self->state);
	timezone_mgr_free(&self->timezone_mgr);
	codec_cache_free(&self->cache_compression);
	buf_mgr_free(&self->buf_mgr);
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
	logger_set_cli(logger, true, true);
	logger_set_to_stdout(logger, true);

	// init ssl library
	tls_lib_init();

	// set UTC time by default (for posix time api)
	setenv("TZ", "UTC", true);

	// read time zones
	timezone_mgr_open(&self->timezone_mgr);

	// set default timezone as system timezone
	self->timezone = self->timezone_mgr.system;
	logger_set_timezone(logger, self->timezone);

	// init uuid manager
	random_open(&self->random);

	// start background job manager
	job_mgr_start(&self->job_mgr, 1);

	// prepare default configuration
	config_prepare(&self->config);

	// prepare state
	state_prepare(&self->state);
}

static void
runtime_shutdown(Runtime* self)
{
	// stop background job manager
	job_mgr_stop(&self->job_mgr);
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
	rc = task_create_nothrow(&self->task, "main", runtime_main, &args, self, NULL,
	                         logger_write, &self->logger,
	                         &self->buf_mgr);
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
