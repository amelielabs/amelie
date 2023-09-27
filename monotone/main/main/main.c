
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_db.h>
#include <monotone_shard.h>
#include <monotone_session.h>
#include <monotone_hub.h>
#include <monotone_core.h>
#include <monotone_main.h>

typedef struct
{
	Str*  directory;
	Str*  options;
	Main* self;
} MainArgs;

static bool
main_prepare(Main* self, MainArgs* args)
{
	bool bootstrap = false;
	auto config = config();

	// init ssl library
	tls_lib_init();

	// init uuid manager
	uuid_mgr_open(&self->uuid_mgr);

	// start resolver
	resolver_start(&self->resolver);

	// prepare default configuration
	config_prepare(config);

	// set directory
	char path[PATH_MAX];
	if (! str_empty(args->directory))
	{
		var_string_set(&config->directory, args->directory);

		// create directory if not exists
		bootstrap = !fs_exists("%s", str_of(args->directory));
		if (bootstrap)
		{
			fs_mkdir(0755, "%s", str_of(args->directory));
		} else
		{
			// read config file
			snprintf(path, sizeof(path), "%s/monotone.conf",
			         str_of(args->directory));
			config_open(config, path);
		}

	} else
	{
		var_int_set(&config->log_to_file, false);
	}

	// set options
	if (! str_empty(args->options))
		config_set(config, false, args->options);

	// configure logger
	auto logger = &self->logger;
	logger_set_enable(logger, var_int_of(&config->log_enable));
	logger_set_to_stdout(logger, var_int_of(&config->log_to_stdout));
	if (var_int_of(&config->log_to_file))
	{
		snprintf(path, sizeof(path), "%s/monotone.log", str_of(args->directory));
		logger_open(logger, path);
	}
	return bootstrap;
}

static void
main_runner(void* arg)
{
	MainArgs* args = arg;
	Main* self = args->self;

	Core* core = NULL;
	Exception e;
	if (try(&e))
	{
		// create base directory and setup logger
		bool bootstrap;
		bootstrap = main_prepare(self, args);

		// start core
		core = core_create();
		core_start(core, bootstrap);

		// notify main_start about start completion
		thread_status_set(&self->task.thread_status, true);

		// handle local connections until stop
		core_main(core);
	}

	if (catch(&e))
	{ }

	// shutdown
	if (core)
	{
		core_stop(core);
		core_free(core);
	}

	// stop resolver
	resolver_stop(&self->resolver);

	thread_status_set(&self->task.thread_status, 0);
}

void
main_init(Main* self)
{
	buf_cache_init(&self->buf_cache);
	logger_init(&self->logger);
	uuid_mgr_init(&self->uuid_mgr);
	resolver_init(&self->resolver, &self->buf_cache);
	config_init(&self->config);
	task_init(&self->task, &self->buf_cache);

	auto global = &self->global;
	global->config   = &self->config;
	global->control  = NULL;
	global->uuid_mgr = &self->uuid_mgr;
	global->resolver = &self->resolver;
}

void
main_free(Main* self)
{
	task_free(&self->task);
	config_free(&self->config);
	uuid_mgr_free(&self->uuid_mgr);
	logger_close(&self->logger);
	buf_cache_free(&self->buf_cache);
	tls_lib_free();
}

int
main_start(Main* self, Str* directory, Str* options)
{
	// start main task
	MainArgs args =
	{
		.directory = directory,
		.options   = options,
		.self      = self
	};
	int rc;
	rc = task_create_nothrow(&self->task, "main", main_runner, &args, &self->global,
	                         logger_write, &self->logger);
	if (unlikely(rc == -1))
		return -1;

	// wait for main task to start
	rc = thread_status_wait(&self->task.thread_status);
	if (unlikely(rc <= 0))
		return -1;

	return 0;
}

void
main_stop(Main* self)
{
	auto buf = msg_create_nothrow(&self->buf_cache, RPC_STOP, 0);
	if (! buf)
		abort();
	channel_write(&self->task.channel, buf);
	task_wait(&self->task);
}

int
main_connect(Main* self, Native* client)
{
	auto buf = native_connect(client, &self->buf_cache);
	if (unlikely(buf == NULL))
		return -1;
	channel_write(&self->task.channel, buf);
	return 0;
}
