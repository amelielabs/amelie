
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_auth.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_session.h>
#include <amelie_main.h>

typedef struct
{
	Str*  options;
	Main* self;
} MainArgs;

static void
main_prepare_listen(void)
{
	Str listen_default;
	str_set_cstr(&listen_default, "[{\"host\": \"*\", \"port\": 3485}]");

	Buf data;
	buf_init(&data);
	guard_buf(&data);

	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, NULL, &listen_default, &data);

	var_data_set_buf(&config()->listen, &data);
}

static bool
main_prepare(Main* self, Str* options)
{
	bool bootstrap = false;
	auto config = config();

	// set UTC time by default
	setenv("TZ", "UTC", true);

	// read time zones
	timezone_mgr_open(&self->timezone_mgr);

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
	var_string_set(&config->directory, &options[MAIN_DIRECTORY]);

	// create directory if not exists
	bootstrap = !fs_exists("%s", config_directory());
	if (bootstrap)
		fs_mkdir(0755, "%s", config_directory());

	// prepare default logger settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	snprintf(path, sizeof(path), "%s/log", config_directory());
	logger_open(logger, path);
	return bootstrap;
}

static void
main_runner(void* arg)
{
	MainArgs* args = arg;
	Main* self = args->self;
	auto options = args->options;

	System* system = NULL;
	Exception e;
	if (enter(&e))
	{
		// create base directory and setup logger
		bool bootstrap;
		bootstrap = main_prepare(self, options);

		// do system restore or start
		if (! str_empty(&options[MAIN_BACKUP]))
		{
			if (! bootstrap)
				error("directory already exists");

			Remote remote;
			remote_init(&remote);
			guard(remote_free, &remote);
			remote_set(&remote, REMOTE_URI, &options[MAIN_BACKUP]);
			remote_set(&remote, REMOTE_FILE_CA, &options[MAIN_BACKUP_CAFILE]);
			remote_set(&remote, REMOTE_TOKEN, &options[MAIN_BACKUP_TOKEN]);
			restore(&remote);
		} else
		{
			system = system_create();
			system_start(system, &options[MAIN_CONFIG], bootstrap);

			// notify main_start about start completion
			thread_status_set(&self->task.thread_status, true);

			// handle system requests
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

	thread_status_set(&self->task.thread_status, !e.triggered);
}

void
main_init(Main* self)
{
	logger_init(&self->logger);
	random_init(&self->random);
	resolver_init(&self->resolver);
	config_init(&self->config);
	timezone_mgr_init(&self->timezone_mgr);
	task_init(&self->task);

	auto global = &self->global;
	global->config       = &self->config;
	global->timezone     = NULL;
	global->timezone_mgr = &self->timezone_mgr;
	global->control      = NULL;
	global->random       = &self->random;
	global->logger       = &self->logger;
	global->resolver     = &self->resolver;
}

void
main_free(Main* self)
{
	task_free(&self->task);
	config_free(&self->config);
	timezone_mgr_free(&self->timezone_mgr);
	random_free(&self->random);
	logger_close(&self->logger);
	tls_lib_free();
}

int
main_start(Main* self, Str* options)
{
	// start main task
	MainArgs args =
	{
		.options = options,
		.self    = self
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
