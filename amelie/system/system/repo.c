
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>
#include <amelie_system.h>

static void
repo_create(Repo* self, char* directory)
{
	// set directory
	opt_string_set_raw(&state()->directory, directory, strlen(directory));

	// create directory if not exists
	self->bootstrap = !fs_exists("{s}", state_directory());
	if (self->bootstrap)
	{
		fs_mkdir(0755, "{s}", state_directory());
		fs_mkdir(0755, "{s}/security", state_directory());
		fs_mkdir(0755, "{s}/checkpoint", state_directory());
	}

	// open directory fd
	self->fd = vfs_open(directory, O_DIRECTORY|O_RDONLY, 0755);
	if (self->fd == -1)
		error_system();

	// take exclusive directory lock
	auto rc = vfs_flock_exclusive(self->fd);
	if (rc == -1)
	{
		if (errno == EWOULDBLOCK)
			error("database already started");
		else
			error_system();
	}
}

static void
repo_pidfile_create(void)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/amelie.pid", state_directory());

	auto fd = vfs_open(path, O_TRUNC|O_CREAT|O_WRONLY, 0644);
	if (fd == -1)
		error_system();

	char pid[32];
	auto pid_len = format(pid, sizeof(pid), "{d}", getpid());

	auto rc = vfs_write(fd, pid, pid_len);
	vfs_close(fd);
	if (rc != pid_len)
		error("pid file '{s}' write error ({s})", path,
		      strerror(errno));
}

static Buf*
repo_listen_create(void)
{
	// []
	auto buf = buf_create();
	encode_array(buf);

	auto config = server_config_allocate();
	defer(server_config_free, config);

	// 127.0.0.1:8080
	Str value;
	str_set_cstr(&value, "127.0.0.1");
	server_config_set_host(config, &value);
	server_config_set_port(config, 8080);
	server_config_write(config, buf, 0);

	// [::1]:8080
	str_set_cstr(&value, "::1");
	server_config_set_host(config, &value);
	server_config_write(config, buf, 0);

	encode_array_end(buf);
	return buf;
}

static void
repo_bootstrap(void)
{
	auto config = config();

	// generate uuid, unless it is set
	if (opt_uuid_empty(&config->uuid))
	{
		Uuid uuid;
		uuid_generate(&uuid, &am_task->random);
		opt_uuid_set(&config->uuid, &uuid);
	}

	// generate secret
	uint8_t secret[32];
	random_generate_alnum(&am_task->random, secret, sizeof(secret));
	opt_string_set_raw(&state()->secret, (char*)secret, sizeof(secret));

	// set default timezone using system timezone
	if (opt_string_empty(&config->timezone))
		opt_string_set(&config->timezone, &runtime()->timezones.system->name);

	// set default listen
	if (opt_json_empty(&config()->listen))
	{
		auto buf = repo_listen_create();
		defer_buf(buf);
		opt_json_set_buf(&config()->listen, buf);
	}
}

static void
repo_open_client_mode(int argc, char** argv)
{
	auto runtime = runtime();
	auto config  = config();

	// prepare logger
	auto logger = &runtime->logger;
	logger_set_enable(logger, true);
	logger_set_stdout(logger, false);

	// set default settings
	repo_bootstrap();

	// set options
	opts_set_argv(&config->opts, argc, argv);

	// set system timezone
	auto name = &config()->timezone.string;
	runtime->timezone = timezones_find(&runtime->timezones, name);
	if (! runtime->timezone)
		error("failed to find timezone {str}", name);

	// reconfigure logger
	logger_set_enable(logger, opt_int_of(&config->log_enable));
	logger_set_timezone(logger, runtime->timezone);
	logger_set_stdout(logger, opt_int_of(&config->log_stdout));
	logger_set_stdout_time(logger, opt_int_of(&config->log_stdout_time));
}

void
repo_init(Repo* self)
{
	self->fd        = -1;
	self->bootstrap = false;
}

void
repo_open(Repo* self, char* directory, int argc, char** argv)
{
	if (! directory)
	{
		repo_open_client_mode(argc, argv);
		self->bootstrap = true;
		return;
	}

	auto runtime = runtime();
	auto config  = config();
	auto state   = state();

	// open or create base directory, take exclusive lock
	repo_create(self, directory);

	// rewrite pid file
	repo_pidfile_create();

	// open log with default settings
	auto logger = &runtime->logger;
	logger_set_enable(logger, true);
	logger_set_stdout(logger, false);
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/log", state_directory());
	logger_open(logger, path);

	// read config file
	format(path, sizeof(path), "{s}/amelie.config", state_directory());
	if (self->bootstrap)
	{
		// set options first, to properly generate config
		opts_set_argv(&config->opts, argc, argv);

		// set default settings
		repo_bootstrap();

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
	format(path, sizeof(path), "{s}/amelie.state", state_directory());
	if (self->bootstrap)
	{
		// create state file
		state_save(state, path);
	} else
	{
		// open state file
		state_open(state, path);

		// validate version
		if (! str_is_cstr(&state()->version.string, AMELIE_VERSION))
			error("repository version {str} does not match the server version {s}",
			      &state()->version.string, AMELIE_VERSION);
	}

	// set system timezone
	auto name = &config()->timezone.string;
	runtime->timezone = timezones_find(&runtime->timezones, name);
	if (! runtime->timezone)
		error("failed to find timezone {str}", name);

	// reconfigure logger
	logger_set_enable(logger, opt_int_of(&config->log_enable));
	logger_set_timezone(logger, runtime->timezone);
	logger_set_stdout(logger, opt_int_of(&config->log_stdout));
	logger_set_stdout_time(logger, opt_int_of(&config->log_stdout_time));
	if (! opt_int_of(&config->log_file))
		logger_close(logger);

	// reconfigure jobs manager
	auto jobs = &runtime()->jobs;
	auto jobs_count = (int)opt_int_of(&config->jobs);
	if (jobs_count != jobs->workers_count)
	{
		jobs_stop(jobs);
		jobs_start(jobs, jobs_count);
	}
}

void
repo_close(Repo* self)
{
	if (self->fd != -1)
	{
		vfs_close(self->fd);
		self->fd = -1;
	}
}
