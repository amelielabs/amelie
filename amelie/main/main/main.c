
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
	info("Usage: amelie [command] [options]");
	info("");
	info("Commands:");
	for (auto i = 0;; i++)
	{
		auto cmd = &main_cmds[i];
		if (! cmd->name)
			break;
		info("  {-10s} {s}", cmd->name, cmd->description);
	}
	info("");
}

void
main_init(Main* self, int argc, char** argv)
{
	self->argc = argc;
	self->argv = argv;
	console_init(&self->console);
	bookmarks_init(&self->bookmarks);
	endpoint_init(&self->endpoint);
	opt_int_set(&self->endpoint.debug, true);
}

void
main_free(Main* self)
{
	console_free(&self->console);
	bookmarks_free(&self->bookmarks);
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
		size = format(path, path_size, "{s}/", home);
	} else
	{
		home = getenv("HOME");
		size = format(path, path_size, "{s}/.amelie/", home);
	}
	va_list args;
	va_start(args, fmt);
	size += formatv(path + size, path_size - size, fmt, args);
	va_end(args);
}

static void
main_load(Main* self)
{
	// create AMELIE_HOME ($HOME/.amelie by default)
	char path[PATH_MAX];
	main_path(path, sizeof(path), "");
	if (! fs_exists("{s}", path))
		fs_mkdir(0755, "{s}", path);

	// read bookmarks
	main_path(path, sizeof(path), "bookmarks");
	bookmarks_open(&self->bookmarks, path);

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
	bookmarks_sync(&self->bookmarks, path);

	// write console history
	main_path(path, sizeof(path), "history");
	console_save(&self->console, path);
}

void
main_open(Main* self, Opts* opts)
{
	// read bookmarks and history
	main_load(self);

	// process command line, set endpoint and options
	main_configure(self, opts);
}

void
main_close(Main* self)
{
	main_save(self);
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
		info("{str}", version);
		return;
	}
	for (auto i = 0;; i++)
	{
		auto cmd = &main_cmds[i];
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
	{
		if (!strcmp(argv[i], "--daemon=true") ||
		    !strcmp(argv[i], "--daemon"))
			daemonize = true;
	}
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
