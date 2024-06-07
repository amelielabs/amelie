
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
#include <sonata_user.h>
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
#include <sonata_repl.h>
#include <sonata_cluster.h>
#include <sonata_frontend.h>
#include <sonata_session.h>
#include <sonata_main.h>

typedef struct
{
	Str*  directory;
	Str*  options;
	Str*  backup;
	Main* self;
} MainArgs;

static void
main_prepare_listen(void)
{
	auto listen = &config()->listen;

	Str listen_default;
	str_set_cstr(&listen_default, "[{\"host\": \"*\", \"port\": 3485}]");

	Buf data;
	buf_init(&data);
	guard_buf(&data);

	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, &listen_default, &data);

	var_data_set_buf(listen, &data);
}

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

	// set default server listen
	main_prepare_listen();

	// set directory
	char path[PATH_MAX];
	var_string_set(&config->directory, args->directory);

	// create directory if not exists
	bootstrap = !fs_exists("%s", config_directory());
	if (bootstrap)
		fs_mkdir(0755, "%s", config_directory());

	// prepare default logger settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_to_stdout(logger, true);
	snprintf(path, sizeof(path), "%s/log", config_directory());
	logger_open(logger, path);
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

		// do system restore or start
		if (args->backup)
		{
			if (! bootstrap)
				error("directory already exists");
			restore(args->backup);
		} else
		{
			system = system_create();
			system_start(system, args->options, bootstrap);

			// notify main_start about start completion
			thread_status_set(&self->task.thread_status, true);

			// handle connections until stop
			system_main(system);
		}
	}

	if (leave(&e))
	{ }

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
	global->logger   = &self->logger;
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
main_start(Main* self, Str* directory, Str* options, Str* backup)
{
	// start main task
	MainArgs args =
	{
		.directory = directory,
		.options   = options,
		.backup    = backup,
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
