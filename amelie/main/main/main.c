
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

#include <amelie>
#include <amelie_main.h>

static void
main_usage(void)
{
	info("Usage: amelie [PATH | URI | BOOKMARK] [command] [OPTIONS]");
	info("");
	info("Commands:");
	for (auto i = 0;; i++)
	{
		auto cmd = &main_commands[i];
		if (! cmd->name)
			break;
		info("  %-10s %s", cmd->name, cmd->description);
	}
	info("");
}

void
main_init(Main* self, int argc, char** argv)
{
	self->access = MAIN_REMOTE;
	self->env    = NULL;
	self->argc   = argc;
	self->argv   = argv;
	console_init(&self->console);
	bookmark_mgr_init(&self->bookmark_mgr);
	endpoint_init(&self->endpoint);
}

void
main_free(Main* self)
{
	console_free(&self->console);
	bookmark_mgr_free(&self->bookmark_mgr);
	endpoint_free(&self->endpoint);
}

static inline void
main_path(char* path, int path_size, char* fmt, ...)
{
	// use AMELIE_HOME ($HOME/.amelie by default)
	int  size = 0;
	auto home = getenv("AMELIE_HOME");
	if (home)
	{
		size = sfmt(path, path_size, "%s/", home);
	} else
	{
		home = getenv("HOME");
		size = sfmt(path, path_size, "%s/.amelie/", home);
	}
	va_list args;
	va_start(args, fmt);
	size += vsfmt(path + size, path_size - size, fmt, args);
	va_end(args);
}

static void
main_load(Main* self)
{
	// create AMELIE_HOME ($HOME/.amelie by default)
	char path[PATH_MAX];
	main_path(path, sizeof(path), "");
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// read bookmarks
	main_path(path, sizeof(path), "bookmarks");
	bookmark_mgr_open(&self->bookmark_mgr, path);

	// read console history
	main_path(path, sizeof(path), "history");
	console_load(&self->console, path);
}

static void
main_save(Main* self)
{
	// write bookmarks
	char path[PATH_MAX];
	main_path(path, sizeof(path), "bookmarks");
	bookmark_mgr_sync(&self->bookmark_mgr, path);

	// write console history
	main_path(path, sizeof(path), "history");
	console_save(&self->console, path);
}

static int
main_access(Main* self)
{
	// choose how to connect
	auto path = opt_string_of(&self->endpoint.path);
	if (str_empty(path))
		return MAIN_REMOTE;

	// path not exists (create on start)
	if (! fs_exists("%s", str_of(path)))
		return MAIN_LOCAL;

	// path exists

	// try to take exclusive directory lock
	auto fd = vfs_open(str_of(path), O_DIRECTORY|O_RDONLY, 0755);
	if (fd == -1)
		error_system();

	// instance already started, choose to connect
	auto result = MAIN_LOCAL;
	auto rc = vfs_flock_exclusive(fd);
	if (rc == -1 && errno == EWOULDBLOCK)
		result = MAIN_REMOTE_PATH;
	vfs_close(fd);
	return result;
}

void
main_open(Main* self, MainOpen type, Opts* opts)
{
	// read bookmarks and history
	main_load(self);
	if (type == MAIN_OPEN_HOME)
		return;

	// process command line, set endpoint and options
	main_configure(self, opts);

	// set the database access type
	self->access = main_access(self);

	// validate access
	switch (type) {
	case MAIN_OPEN_CONFIGURE:
		return;
	case MAIN_OPEN_REMOTE:
	{
		// allow only remote connection by uri or using unix socket path to
		// the active database
		if (self->access == MAIN_LOCAL)
			error("database is not running");

		// MAIN_REMOTE_PATH
		// MAIN_REMOTE
		return;
	}
	case MAIN_OPEN_LOCAL_RUNNING:
	{
		// ensure local database is running
		if (self->access == MAIN_REMOTE)
			error("database path is not set");

		if (self->access == MAIN_LOCAL)
			error("database is not started");

		// MAIN_REMOTE_PATH
		return;
	}
	case MAIN_OPEN_LOCAL:
	case MAIN_OPEN_LOCAL_NEW:
	{
		// allow only database open
		if (self->access == MAIN_REMOTE_PATH)
			error("database is already started");

		if (self->access == MAIN_REMOTE)
			error("database path is not set");

		// ensure directory does not exists
		if (type == MAIN_OPEN_LOCAL_NEW)
		{
			auto path = opt_string_of(&self->endpoint.path);
			if (fs_exists("%s", str_of(path)))
				error("database directory already exists");
		}
		break;
	}
	default:
		break;
	}

	// open database to work locally
	if (self->access == MAIN_LOCAL)
	{
		self->env = amelie_init();
		if (! self->env)
			error("amelie_init() failed");
		auto path = opt_string_of(&self->endpoint.path);

		auto argc = self->argc;
		auto argv = self->argv;
		if (type == MAIN_OPEN_ANY_NOOPTS)
		{
			argc = 0;
			argv = NULL;
		}
		auto rc = amelie_open(self->env, str_of(path), argc, argv);
		if (rc == -1)
			error("amelie_open() failed");
	}
}

void
main_close(Main* self)
{
	main_save(self);

	// shutdown database (all sessions must be closed)
	if (self->env)
	{
		amelie_free(self->env);
		self->env = NULL;
	}
}

static void
main_entry(Main* self)
{
	// amelie [command] [options]
	auto argc = self->argc;
	auto argv = self->argv;
	if (argc <= 1 || !strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))
	{
		main_usage();
		return;
	}
	if (!strcmp(argv[1], "-v") ||
	    !strcmp(argv[1], "--version"))
	{
		auto version = &state()->version.string;
		info("%.*s", str_size(version), str_of(version));
		return;
	}
	for (auto i = 0;; i++)
	{
		auto cmd = &main_commands[i];
		if (! cmd->name)
			break;
		if (strcmp(cmd->name, argv[1]) != 0)
			continue;
		main_advance(self, 2);
		cmd->function(self);
		return;
	}
	main_advance(self, 1);
	main_cli(self);
}

void
main_runtime(void* arg, int argc, char** argv)
{
	unused(arg);

	// runtime main function
	Main main;
	main_init(&main, argc, argv);
	defer(main_free, &main);
	main_entry(&main);
}

static void
main_wait_for_signal(void)
{
	// wait signal for completion
	sigset_t mask;
	sigfillset(&mask);
	pthread_sigmask(SIG_BLOCK, &mask, NULL);
	sigemptyset(&mask);
	sigaddset(&mask, SIGINT);
	sigaddset(&mask, SIGTERM);
	int signo;
	sigwait(&mask, &signo);
}

static int
main_daemonize(int argc, char** argv)
{
	// amelie start .. --daemon=
	if (argc <= 1 || strcmp(argv[1], "start") != 0)
		return 0;
	auto daemonize = false;
	for (auto i = 2; i < argc; i++)
		if (! strcmp(argv[i], "--daemon=true"))
			daemonize = true;
	if (! daemonize)
		return 0;
	return daemon(1, 0);
}

int
main(int argc, char* argv[])
{
	if (main_daemonize(argc, argv) == -1)
		return EXIT_FAILURE;
	Runtime runtime;
	runtime_init(&runtime);
	auto status = runtime_start(&runtime, main_runtime, NULL, argc, argv);
	if (status == RUNTIME_OK)
		main_wait_for_signal();
	runtime_stop(&runtime);
	runtime_free(&runtime);
	return status == RUNTIME_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
