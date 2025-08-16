
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
env_init(Env* self)
{
	self->timezone = NULL;
	self->crc      = crc32_sse_supported() ? crc32_sse : crc32;
	self->control  = NULL;
	buf_mgr_init(&self->buf_mgr);
	config_init(&self->config);
	state_init(&self->state);
	timezone_mgr_init(&self->timezone_mgr);
	random_init(&self->random);
	resolver_init(&self->resolver);
	logger_init(&self->logger);
}

void
env_free(Env* self)
{
	config_free(&self->config);
	state_free(&self->state);
	timezone_mgr_free(&self->timezone_mgr);
	buf_mgr_free(&self->buf_mgr);
	logger_close(&self->logger);
	tls_lib_free();
}

void
env_start(Env* self)
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
	config_prepare(config());

	// prepare state
	state_prepare(state());
}

void
env_stop(Env* self)
{
	// stop resolver
	resolver_stop(&self->resolver);
}

bool
env_create(Env* self, char* directory)
{
	unused(self);

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
env_version_create(void)
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
env_version_save(const char* path)
{
	auto buf = env_version_create();
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
env_version_open(const char* path)
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
env_bootstrap_server(void)
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
env_bootstrap(Env* self)
{
	auto config = config();
	unused(self);

	// generate uuid, unless it is set
	if (! opt_string_is_set(&config->uuid))
	{
		Uuid uuid;
		uuid_generate(&uuid, &self->random);
		char uuid_sz[UUID_SZ];
		uuid_get(&uuid, uuid_sz, sizeof(uuid_sz));
		opt_string_set_raw(&config->uuid, uuid_sz, sizeof(uuid_sz) - 1);
	}

	// set default timezone using system timezone
	if (! opt_string_is_set(&config->timezone))
		opt_string_set(&config->timezone, &self->timezone_mgr.system->name);

	// set default server listen
	if (! opt_json_is_set(&config->listen))
		env_bootstrap_server();
}

bool
env_open(Env* self, char* directory, int argc, char** argv)
{
	auto config = &self->config;
	auto state  = &self->state;

	// create base directory, if not exists
	auto bootstrap = env_create(self, directory);

	// open log with default settings
	auto logger = &self->logger;
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
		env_version_save(path);
	} else
	{
		// read and validate version file
		env_version_open(path);
	}

	// read config file
	snprintf(path, sizeof(path), "%s/config.json", state_directory());
	if (bootstrap)
	{
		// set options first, to properly generate config
		opts_set_argv(&config->opts, argc, argv);

		// set default settings
		env_bootstrap(self);

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
	self->timezone = timezone_mgr_find(&self->timezone_mgr, name);
	if (! self->timezone)
		error("failed to find timezone %.*s", str_size(name), str_of(name));

	// reconfigure logger
	logger_set_enable(logger, opt_int_of(&config->log_enable));
	logger_set_to_stdout(logger, opt_int_of(&config->log_to_stdout));
	logger_set_timezone(logger, self->timezone);
	if (! opt_int_of(&config->log_to_file))
		logger_close(logger);

	// validate compression type
	auto cp = &config()->checkpoint_compression.string;
	if (!str_is(cp, "zstd", 4) && !str_is(cp, "none", 4))
		error("invalid checkpoint_compression type %.*s",
		      str_size(cp), str_of(cp));

	return bootstrap;
}
