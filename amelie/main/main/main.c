
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
	info("Usage: amelie [command] [uri | path] [options]");
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
	endpoint_init(&self->endpoint);
	opt_int_set(&self->endpoint.debug, true);
}

void
main_free(Main* self)
{
	console_free(&self->console);
	endpoint_free(&self->endpoint);
}

static void
main_load(Main* self)
{
	if (! self->home)
		return;

	// $HOME/.amelie_history
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/.amelie_history", getenv("HOME"));
	console_load(&self->console, path);
}

static void
main_save(Main* self)
{
	if (! self->home)
		return;

	// write console history
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/.amelie_history", getenv("HOME"));
	console_save(&self->console, path);
}

void
main_configure(Main* self)
{
	auto endpoint = &self->endpoint;
	auto argc     = self->argc;
	auto argv     = self->argv;

	if (argc == 0)
		error("uri or path expected");

	// [path, uri]
	int arg = 0;
	if (!strncmp(argv[0], ".", 1) || !strncmp(argv[0], "/", 1))
	{
		// path
		Str path;
		str_set_cstr(&path, argv[0]);
		opt_string_set(&endpoint->path, &path);
		arg = 1;
	} else
	if (strncmp(argv[0], "--", 2) != 0)
	{
		// parse uri
		Str uri;
		str_set_cstr(&uri, argv[0]);
		uri_parse(endpoint, &uri);
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

	// set token
	endpoint_auth(endpoint);

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
		info("{s}", AMELIE_VERSION);
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

	// read history
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
