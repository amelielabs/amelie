
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
	resolver_init(&self->resolver);
	logger_init(&self->logger);
	task_init(&self->task);
}

void
runtime_free(Runtime* self)
{
	task_free(&self->task);
	resolver_free(&self->resolver);
	config_free(&self->config);
	state_free(&self->state);
	timezone_mgr_free(&self->timezone_mgr);
	buf_mgr_free(&self->buf_mgr);
	logger_close(&self->logger);
	tls_lib_free();
}

typedef struct
{
	RuntimeMain main;
	char*       directory;
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

	// start resolver
	resolver_start(&self->resolver);

	// prepare default configuration
	config_prepare(&self->config);

	// prepare state
	state_prepare(&self->state);
}

static void
runtime_shutdown(Runtime* self)
{
	// stop resolver
	resolver_stop(&self->resolver);
}

static void
runtime_main(void* arg)
{
	RuntimeArgs* args = arg;
	auto self = runtime();
	auto on_error = error_catch
	(
		runtime_prepare(self);
		args->main(args->directory, args->argc, args->argv);
	);
	runtime_shutdown(self);

	// notify for completion
	RuntimeStatus status = RUNTIME_COMPLETE;
	if (on_error)
		status = RUNTIME_ERROR;
	cond_signal(&self->task.status, status);
}

RuntimeStatus
runtime_start(Runtime* self, RuntimeMain main, char* directory, int argc, char** argv)
{
	RuntimeArgs args =
	{
		.main      = main,
		.directory = directory,
		.argc      = argc,
		.argv      = argv,
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
		channel_write(&self->task.channel, &stop);
		task_wait(&self->task);
	}
}
