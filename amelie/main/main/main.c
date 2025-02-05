
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

#include <amelie_private.h>

void
main_init(Main* self)
{
	logger_init(&self->logger);
	random_init(&self->random);
	resolver_init(&self->resolver);
	config_init(&self->config);
	timezone_mgr_init(&self->timezone_mgr);
	buf_mgr_init(&self->buf_mgr);

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
	config_free(&self->config);
	timezone_mgr_free(&self->timezone_mgr);
	buf_mgr_free(&self->buf_mgr);
	logger_close(&self->logger);
	tls_lib_free();
}

void
main_start(Main* self)
{
	// prepare default logger settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_cli(logger, true);
	logger_set_to_stdout(logger, true);

	// init ssl library
	tls_lib_init();

	// set UTC time by default (for posix time api)
	setenv("TZ", "UTC", true);

	// read time zones
	timezone_mgr_open(&self->timezone_mgr);

	// set default timezone as system timezone
	global()->timezone = self->timezone_mgr.system;
	logger_set_timezone(logger, global()->timezone);

	// init uuid manager
	random_open(&self->random);

	// start resolver
	resolver_start(&self->resolver);

	// prepare default configuration
	config_prepare(config());
}

void
main_stop(Main* self)
{
	// stop resolver
	resolver_stop(&self->resolver);
}

bool
main_create(Main* self, char* directory)
{
	unused(self);

	// set directory
	var_string_set_raw(&config()->directory, directory, strlen(directory));

	// create directory if not exists
	auto bootstrap = !fs_exists("%s", config_directory());
	if (bootstrap)
	{
		fs_mkdir(0755, "%s", config_directory());
		fs_mkdir(0755, "%s/certs", config_directory());
	}

	return bootstrap;
}

static void
main_bootstrap(Main* self)
{
	auto config = config();
	unused(self);

	// generate uuid, unless it is set
	if (! var_string_is_set(&config->uuid))
	{
		Uuid uuid;
		uuid_generate(&uuid, global()->random);
		char uuid_sz[UUID_SZ];
		uuid_get(&uuid, uuid_sz, sizeof(uuid_sz));
		var_string_set_raw(&config->uuid, uuid_sz, sizeof(uuid_sz) - 1);
	}

	// set default timezone using system timezone
	if (! var_string_is_set(&config->timezone_default))
		var_string_set(&config->timezone_default, &self->timezone_mgr.system->name);

	// set default server listen
	if (! var_json_is_set(&config->listen))
		server_bootstrap();
}

bool
main_open(Main* self, char* directory, int argc, char** argv)
{
	auto config = config();

	// create base directory, if not exists
	auto bootstrap = main_create(self, directory);

	// open log with default settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_cli(logger, false);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/log", config_directory());
	logger_open(logger, path);

	// read config
	snprintf(path, sizeof(path), "%s/config.json", config_directory());
	if (bootstrap)
	{
		// set options first, to properly generate config
		vars_set_argv(&config->vars, argc, argv);

		// set default settings
		main_bootstrap(self);

		// create config file
		config_open(config, path);
	} else
	{
		// open config file
		config_open(config, path);

		// redefine options and update config if necessary
		if (vars_set_argv(&config->vars, argc, argv))
			config_save(config, path);
	}

	// validate shutdown mode
	auto shutdown = &config()->shutdown.string;
	if (!str_is_cstr(shutdown, "fast") &&
	    !str_is_cstr(shutdown, "graceful"))
		error("unrecognized shutdown mode %.*s", str_size(shutdown),
		      str_of(shutdown));

	// set system timezone
	auto name = &config()->timezone_default.string;
	global()->timezone = timezone_mgr_find(global()->timezone_mgr, name);
	if (! global()->timezone)
		error("failed to find timezone %.*s", str_size(name), str_of(name));

	// reconfigure logger
	logger_set_enable(logger, var_int_of(&config->log_enable));
	logger_set_to_stdout(logger, var_int_of(&config->log_to_stdout));
	logger_set_timezone(logger, global()->timezone);
	if (! var_int_of(&config->log_to_file))
		logger_close(logger);

	return bootstrap;
}
