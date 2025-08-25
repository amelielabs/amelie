
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

static bool
repository_create(char* directory)
{
	// set directory
	opt_string_set_raw(&state()->directory, directory, strlen(directory));

	// create directory if not exists
	auto bootstrap = !fs_exists("%s", state_directory());
	if (bootstrap)
	{
		fs_mkdir(0755, "%s", state_directory());
		fs_mkdir(0755, "%s/certs", state_directory());
	}

	return bootstrap;
}

static Buf*
repository_version_create(void)
{
	// {}
	auto buf = buf_create();
	encode_obj(buf);

	// version
	encode_raw(buf, "version", 7);
	encode_string(buf, &state()->version.string);

	encode_obj_end(buf);
	return buf;
}

static void
repository_version_save(const char* path)
{
	auto buf = repository_version_create();
	defer_buf(buf);

	// convert to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	uint8_t* pos = buf->start;
	json_export_pretty(&text, NULL, &pos);

	// create config file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	file_write_buf(&file, &text);
}

static void
repository_version_open(const char* path)
{
	// read version file
	auto version_buf = file_import("%s", path);
	defer_buf(version_buf);

	Str text;
	str_init(&text);
	buf_str(version_buf, &text);

	// parse json
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &text, NULL);
	uint8_t* pos = json.buf->start;

	Str version;
	str_init(&version);
	Decode obj[] =
	{
		{ DECODE_STRING_READ, "version", &version },
		{ 0,                   NULL,      NULL    },
	};
	decode_obj(obj, "version", &pos);

	// compare versions
	auto version_current = &state()->version.string;
	if (! str_compare(&version, version_current))
		error("current version '%.*s' does not match repository version '%.*s'",
		      str_size(version_current),
		      str_of(version_current),
		      str_size(&version),
		      str_of(&version));
}

static void
repository_bootstrap_server(void)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	// tls and auto are disabled by default

	// []
	encode_array(&buf);

	// {}
	encode_obj(&buf);
	// host
	encode_raw(&buf, "host", 4);
	encode_cstr(&buf, "*");
	// port
	encode_raw(&buf, "port", 4);
	encode_integer(&buf, 3485);
	encode_obj_end(&buf);

	encode_array_end(&buf);

	opt_json_set_buf(&config()->listen, &buf);
}

static void
repository_bootstrap(void)
{
	auto config = config();

	// generate uuid, unless it is set
	if (! opt_string_is_set(&config->uuid))
	{
		Uuid uuid;
		uuid_generate(&uuid, &runtime()->random);
		char uuid_sz[UUID_SZ];
		uuid_get(&uuid, uuid_sz, sizeof(uuid_sz));
		opt_string_set_raw(&config->uuid, uuid_sz, sizeof(uuid_sz) - 1);
	}

	// set default timezone using system timezone
	if (! opt_string_is_set(&config->timezone))
		opt_string_set(&config->timezone, &runtime()->timezone_mgr.system->name);

	// set default server listen
	if (! opt_json_is_set(&config->listen))
		repository_bootstrap_server();
}

static void
repository_open_client_mode(int argc, char** argv)
{
	auto runtime = runtime();
	auto config  = config();

	// prepare logger
	auto logger = &runtime->logger;
	logger_set_enable(logger, true);
	logger_set_cli(logger, false, false);

	// set default settings
	repository_bootstrap();

	// set options
	opts_set_argv(&config->opts, argc, argv);

	// set system timezone
	auto name = &config()->timezone.string;
	runtime->timezone = timezone_mgr_find(&runtime->timezone_mgr, name);
	if (! runtime->timezone)
		error("failed to find timezone %.*s", str_size(name), str_of(name));

	// reconfigure logger
	logger_set_enable(logger, opt_int_of(&config->log_enable));
	logger_set_to_stdout(logger, opt_int_of(&config->log_to_stdout));
	logger_set_timezone(logger, runtime->timezone);
}

bool
repository_open(char* directory, int argc, char** argv)
{
	if (! directory)
	{
		repository_open_client_mode(argc, argv);
		return true;
	}

	auto runtime = runtime();
	auto config  = config();
	auto state   = state();

	// create base directory, if not exists
	auto bootstrap = repository_create(directory);

	// open log with default settings
	auto logger = &runtime->logger;
	logger_set_enable(logger, true);
	logger_set_cli(logger, false, false);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/log", state_directory());
	logger_open(logger, path);

	// read version file
	snprintf(path, sizeof(path), "%s/version.json", state_directory());
	if (bootstrap)
	{
		// create version file
		repository_version_save(path);
	} else
	{
		// read and validate version file
		repository_version_open(path);
	}

	// read config file
	snprintf(path, sizeof(path), "%s/config.json", state_directory());
	if (bootstrap)
	{
		// set options first, to properly generate config
		opts_set_argv(&config->opts, argc, argv);

		// set default settings
		repository_bootstrap();

		// create config file
		config_save(config, path);
	} else
	{
		// open config file
		config_open(config, path);

		// redefine options and update config if necessary
		opts_set_argv(&config->opts, argc, argv);
	}

	// read state file
	snprintf(path, sizeof(path), "%s/state.json", state_directory());
	if (bootstrap)
	{
		// create state file
		state_save(state, path);
	} else
	{
		// open state file
		state_open(state, path);
	}

	// set system timezone
	auto name = &config()->timezone.string;
	runtime->timezone = timezone_mgr_find(&runtime->timezone_mgr, name);
	if (! runtime->timezone)
		error("failed to find timezone %.*s", str_size(name), str_of(name));

	// reconfigure logger
	logger_set_enable(logger, opt_int_of(&config->log_enable));
	logger_set_to_stdout(logger, opt_int_of(&config->log_to_stdout));
	logger_set_timezone(logger, runtime->timezone);
	if (! opt_int_of(&config->log_to_file))
		logger_close(logger);

	// validate compression type
	auto cp = &config()->checkpoint_compression.string;
	if (!str_is(cp, "zstd", 4) && !str_is(cp, "none", 4))
		error("invalid checkpoint_compression type %.*s",
		      str_size(cp), str_of(cp));

	return bootstrap;
}
