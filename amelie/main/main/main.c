
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
	info("Usage: amelie [command] [path | uri | bookmark] [options]");
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
	self->home = false;
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
	if (! self->home)
		return;

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
	if (! self->home)
		return;

	// write bookmarks
	char path[PATH_MAX];
	main_path(path, sizeof(path), "bookmarks");
	bookmarks_sync(&self->bookmarks, path);

	// write console history
	main_path(path, sizeof(path), "history");
	console_save(&self->console, path);
}

void
main_configure(Main* self)
{
	auto endpoint = &self->endpoint;
	auto argc     = self->argc;
	auto argv     = self->argv;

	if (argc == 0)
		error("path, uri or bookmark expected");

	// [path, uri or bookmark]
	int arg = 0;
	if (!strncmp(argv[0], ".", 1) ||
	    !strncmp(argv[0], "/", 1))
	{
		// directory path
		Str str;
		str_set_cstr(&str, argv[0]);
		opt_string_set(&endpoint->path, &str);
		arg = 1;
	} else
	if (!strncmp(argv[0], "http://", 7) ||
	    !strncmp(argv[0], "https://", 8))
	{
		// parse uri
		Str uri;
		str_set_cstr(&uri, argv[0]);
		uri_parse(endpoint, &uri);
		arg = 1;
	} else
	if (strncmp(argv[0], "--", 2) != 0)
	{
		// find bookmark by name
		Str name;
		str_set_cstr(&name, argv[0]);
		auto match = bookmarks_find(&self->bookmarks, &name);
		if (match)
		{
			// use bookmark settings
			endpoint_copy(endpoint, &match->endpoint);
		} else
		{
			// use as directory path
			if (str_is_prefix(&name, "/", 1)  ||
			    str_is_prefix(&name, "./", 2) ||
			    str_is_prefix(&name, "../", 3))
				opt_string_set(&endpoint->path, &name);
		}
		arg = 1;
	}

	// [endpoint options | options] ...

	// --<option>=<value> | name
	for (; arg < argc; arg++)
	{
		// stop of first non -- argument
		if (strncmp(argv[arg], "--", 2) != 0)
			break;

		Str name;
		Str value;
		if (arg_parse(argv[arg], &name, &value) == -1)
			error("invalid argument '{s}'", argv[arg]);

		// --json={options}
		if (str_is_cstr(&name, "json"))
		{
			opts_set(&endpoint->opts, &value);
			continue;
		}

		// find endpoint or supplied options
		auto opt = opts_find(&endpoint->opts, &name);
		if (opt)
		{
			if (! opt_is(opt, OPT_C))
				error("argument '{s}' cannot be changed", argv[arg]);
			opt_set(opt, &value);
			continue;
		}

		// stop on first unknown option
		break;
	}

	// validate connection string
	auto uri  = opt_string_of(&endpoint->uri);
	auto path = opt_string_of(&endpoint->path);
	if (str_empty(uri) && str_empty(path))
		error("uri or path is not defined");

	// point to the options
	main_advance(self, arg);
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

	main_advance(self, 1);

	// find command
	MainCmd* command = NULL;
	for (auto i = 0;; i++)
	{
		auto cmd = &main_cmds[i];
		if (! cmd->name)
			break;
		if (strcmp(cmd->name, argv[1]) != 0)
			continue;
		command = cmd;
		main_advance(self, 1);
		break;
	}

	// cli (default)
	if (! command)
	{
		self->home = true;

		main_load(self);
		defer(main_save, self);
		main_configure(self);
		main_cli(self);
		return;
	}

	// read bookmarks and history
	self->home = command->home;

	main_load(self);
	defer(main_save, self);

	// execute command
	if (command->configure)
		main_configure(self);

	command->function(self);
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
