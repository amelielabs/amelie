
//
// amelie.
//
// Real-Time SQL Database.
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
	random_free(&self->random);
	logger_close(&self->logger);
	tls_lib_free();
}

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

void
main_start(Main* self)
{
	// init ssl library
	tls_lib_init();

	// set UTC time by default
	setenv("TZ", "UTC", true);

	// read time zones
	timezone_mgr_open(&self->timezone_mgr);

	// init uuid manager
	random_open(&self->random);

	// start resolver
	resolver_start(&self->resolver);

	// prepare default configuration
	config_prepare(config());

	// set default server listen
	main_prepare_listen();

	// prepare default logger settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_cli(logger, true);
	logger_set_to_stdout(logger, true);
}

bool
main_open(Main* self, char* directory, bool cli)
{
	// set directory
	char path[PATH_MAX];
	var_string_set_raw(&config()->directory, directory, strlen(directory));

	// create directory if not exists
	auto bootstrap = !fs_exists("%s", config_directory());
	if (bootstrap)
		fs_mkdir(0755, "%s", config_directory());

	// open log with default settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_cli(logger, cli);
	snprintf(path, sizeof(path), "%s/log", config_directory());
	logger_open(logger, path);
	return bootstrap;
}

void
main_stop(Main* self)
{
	// stop resolver
	resolver_stop(&self->resolver);
}
