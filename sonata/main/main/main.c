
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_shard.h>
#include <sonata_frontend.h>
#include <sonata_session.h>
#include <sonata_main.h>

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
	random_open(&self->random);

	// start resolver
	resolver_start(&self->resolver);

	// prepare default configuration
	config_prepare(config);

	// set directory
	char path[PATH_MAX];
	var_string_set(&config->directory, args->directory);

	// prepare logger
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_to_stdout(logger, true);

	// create directory if not exists
	bootstrap = !fs_exists("%s", str_of(args->directory));
	if (bootstrap)
	{
		fs_mkdir(0755, "%s", config_directory());

		// set options first, to properly generate config
		if (! str_empty(args->options))
			config_set(config, args->options);

		// generate uuid, unless it is set
		if (! var_string_is_set(&config()->uuid))
		{
			Uuid uuid;
			uuid_generate(&uuid, &self->random);
			char uuid_sz[UUID_SZ];
			uuid_to_string(&uuid, uuid_sz, sizeof(uuid_sz));
			var_string_set_raw(&config()->uuid, uuid_sz, sizeof(uuid_sz) - 1);
		}

		// create config file
		snprintf(path, sizeof(path), "%s/config.json", config_directory());
		config_open(config, path);

	} else
	{
		// open config file
		snprintf(path, sizeof(path), "%s/config.json", config_directory());
		config_open(config, path);

		// redefine options
		if (! str_empty(args->options))
			config_set(config, args->options);
	}

	// configure logger
	logger_set_enable(logger, var_int_of(&config->log_enable));
	logger_set_to_stdout(logger, var_int_of(&config->log_to_stdout));
	if (var_int_of(&config->log_to_file))
	{
		snprintf(path, sizeof(path), "%s/log", str_of(args->directory));
		logger_open(logger, path);
	}

	return bootstrap;
}

static void
main_runner(void* arg)
{
	MainArgs* args = arg;
	Main* self = args->self;

	System* system = NULL;
	Exception e;
	if (enter(&e))
	{
		// create base directory and setup logger
		bool bootstrap;
		bootstrap = main_prepare(self, args);

		// start system
		system = system_create();
		system_start(system, bootstrap);

		// notify main_start about start completion
		thread_status_set(&self->task.thread_status, true);

		// handle connections until stop
		system_main(system);
	}

	if (leave(&e)) {
		log("error: %s", so_self()->error.text);
	}

	// shutdown
	if (system)
	{
		system_stop(system);
		system_free(system);
	}

	// stop resolver
	resolver_stop(&self->resolver);

	thread_status_set(&self->task.thread_status, 0);
}

void
main_init(Main* self)
{
	logger_init(&self->logger);
	random_init(&self->random);
	resolver_init(&self->resolver);
	config_init(&self->config);
	task_init(&self->task);

	auto global = &self->global;
	global->config   = &self->config;
	global->control  = NULL;
	global->random   = &self->random;
	global->resolver = &self->resolver;
}

void
main_free(Main* self)
{
	task_free(&self->task);
	config_free(&self->config);
	random_free(&self->random);
	logger_close(&self->logger);
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
	auto buf = msg_create_nothrow(&self->task.buf_cache, RPC_STOP, 0);
	if (! buf)
		abort();
	channel_write(&self->task.channel, buf);
	task_wait(&self->task);
}
