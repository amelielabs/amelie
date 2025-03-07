
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

#include <amelie.h>

static void
main_usage(void)
{
	auto version = &state()->version.string;
	info("amelie (version: %.*s)", str_size(version), str_of(version));
	info("");
	info("usage: amelie [command | login] [options]");
	info("");
	info("  commands:");
	info("");
	info("    init   <path> [server options]");
	info("    start  <path> [server options]");
	info("    stop   <path>");
	info("    backup <path> [login] [client options]");
	info("    client [login] [client options]");
	info("    import [login] [client options] [options] table files");
	info("    top    [login] [client options]");
	info("    bench  [login] [client options] [options]");
	info("    login  <login> [client options]");
	info("    logout <login>");
	info("");
	info("  client options:");
	info("");
	int id = 0;
	for (; id < REMOTE_MAX; id++)
		info("    --%s=string", remote_nameof(id));
	info("    --json=string");
	info("");
	info("  server options:");
	info("");
	list_foreach(&config()->vars.list)
	{
		auto var = list_at(Var, link);
		if (!var_is(var, VAR_C) || var_is(var, VAR_S))
			continue;
		char* type;
		switch (var->type) {
		case VAR_BOOL: type = "bool";
			break;
		case VAR_INT: type = "int";
			break;
		case VAR_STRING: type = "string";
			break;
		case VAR_JSON: type = "json";
			break;
		}
		info("    --%.*s=%s", str_size(&var->name),
		     str_of(&var->name), type);
	}
	info("    --json=string");
	info("");
	info("  bench options:");
	info("");
	info("    --type=string");
	info("    --threads=int");
	info("    --clients=int");
	info("    --time=int");
	info("    --scale=int");
	info("    --batch=int");
	info("    --init=bool");
	info("    --unlogged=bool");
	info("");
	info("  import options:");
	info("");
	info("    --format=string");
	info("    --clients=int");
	info("    --batch=int");
	info("");
}

static void
main_cmd_init(Main* self, int argc, char** argv)
{
	// amelie init path [server options]
	auto bootstrap = instance_open(&self->instance, argv[0], argc - 1, argv + 1);
	if (! bootstrap)
		error("directory already exists");

	// start, bootstrap and quick exit
	System* system = NULL;
	error_catch
	(
		system = system_create();
		system_start(system, bootstrap);

		// start pending coroutines before stop
		coroutine_sleep(0);
	);

	if (system)
	{
		system_stop(system);
		system_free(system);
	}
}

static void
main_cmd_start(Main* self, int argc, char** argv)
{
	// amelie start path [server options]
	auto bootstrap = instance_open(&self->instance, argv[0], argc - 1, argv + 1);

	System* system = NULL;
	error_catch
	(
		system = system_create();
		system_start(system, bootstrap);

		// notify main_start about start completion
		cond_signal(&self->task.status, MAIN_RUN);

		// handle system requests
		system_main(system);
	);

	if (system)
	{
		system_stop(system);
		system_free(system);
	}
}

static void
main_cmd_stop(Main* self, int argc, char** argv)
{
	// amelie stop path
	(void)self;
	(void)argc;
	(void)argv;
}

static void
main_cmd_backup(Main* self, int argc, char** argv)
{
	home_open(&self->home);

	// amelie backup path [remote options]
	auto bootstrap = instance_create(&self->instance, argv[0]);
	if (! bootstrap)
		error("directory already exists");

	// prepare remote
	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);
	login_mgr_set(&self->home.login_mgr, &remote, NULL, argc - 1, argv + 1);

	// disable log output
	if (str_is_cstr(remote_get(&remote, REMOTE_DEBUG), "0"))
		logger_set_to_stdout(&self->instance.logger, false);

	restore(&remote);
}

static void
main_cmd_client_execute(Client* client, Str* content)
{
	auto request = &client->request;
	auto reply   = &client->reply;
	auto token   = remote_get(client->remote, REMOTE_TOKEN);

	if (str_empty(content))
		return;

	// request
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Type", "text/plain");
	http_write(request, "Content-Length", "%d", str_size(content));
	http_write_end(request);
	tcp_write_pair_str(&client->tcp, &request->raw, content);

	// reply
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &client->readahead, &reply->content);

	// 403 Forbidden
	if (str_is(&reply->options[HTTP_CODE], "403", 3))
	{
		auto code = &reply->options[HTTP_CODE];
		auto msg  = &reply->options[HTTP_MSG];
		printf("%.*s %.*s\n", str_size(code), str_of(code),
			   str_size(msg), str_of(msg));
		return;
	}

	// print
	if (! str_is(&reply->options[HTTP_CODE], "204", 3))
		printf("%.*s\n", buf_size(&reply->content), reply->content.start);
}

static void
main_cmd_client_main(Main* self, Client* client)
{
	auto name = remote_get(client->remote, REMOTE_NAME);
	auto uri  = remote_get(client->remote, REMOTE_URI);
	auto path = remote_get(client->remote, REMOTE_PATH);

	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);

	Str* prompt_str;
	if (! str_empty(name))
		prompt_str = name;
	else
	if (! str_empty(path))
		prompt_str = path;
	else
		prompt_str = uri;
	char prompt_ss[128];
	char prompt_ms[128];
	snprintf(prompt_ss, sizeof(prompt_ss), "%.*s> ", str_size(prompt_str),
	         str_of(prompt_str));
	snprintf(prompt_ms, sizeof(prompt_ms), "%.*s- ", str_size(prompt_str),
	         str_of(prompt_str));

	auto is_terminal = cli_is_terminal();
	for (;;)
	{
		// >
		Str input;
		str_init(&input);
		auto prompt = separator_pending(&sep) ? prompt_ms: prompt_ss;
		if (! cli(prompt, &input))
			break;
		defer(str_free, &input);

		// split commands by \n
		if (is_terminal)
		{
			main_cmd_client_execute(client, &input);
			continue;
		}

		// pipe mode

		// split commands using ; and begin/commit stmts
		Str content;
		separator_write(&sep, &input);
		while (separator_read(&sep, &content))
		{
			main_cmd_client_execute(client, &content);
			separator_advance(&sep);
		}
	}

	home_sync(&self->home);
}

static void
main_cmd_client(Main* self, int argc, char** argv)
{
	// amelie client [remote options]
	home_open(&self->home);
	var_int_set(&config()->log_connections, false);

	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);

	Client* client = NULL;
	error_catch
	(
		// prepare remote
		login_mgr_set(&self->home.login_mgr, &remote, NULL, argc, argv);

		// create client and connect
		client = client_create();
		client_set_remote(client, &remote);
		client_connect(client);

		// process cli
		main_cmd_client_main(self, client);
	);

	if (client)
	{
		client_close(client);
		client_free(client);
	}
}

static void
main_cmd_import(Main* self, int argc, char** argv)
{
	// amelie import name
	auto home = &self->home;
	home_open(home);
	var_int_set(&config()->log_connections, false);

	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);

	Import import;
	import_init(&import, &remote);
	defer(import_free, &import);

	error_catch
	(
		// read arguments
		auto last = login_mgr_set(&self->home.login_mgr, &remote,
		                          &import.vars,
		                           argc,
		                           argv);
		argc -= last;
		argv += last;
		import_run(&import, argc, argv);
	);
}

static void
main_cmd_login(Main* self, int argc, char** argv)
{
	// amelie login name [remote options]
	auto home = &self->home;
	home_open(home);

	Str name;
	str_set_cstr(&name, argv[0]);
	auto login = login_mgr_find(&home->login_mgr, &name);
	if (! login)
	{
		login = login_allocate();
		remote_set(&login->remote, REMOTE_NAME, &name);
		login_mgr_add(&home->login_mgr, login);
	}

	login_mgr_set(&home->login_mgr, &login->remote, NULL, argc - 1, argv + 1);
	home_sync(home);
}

static void
main_cmd_logout(Main* self, int argc, char** argv)
{
	// amelie logout name
	auto home = &self->home;
	home_open(home);
	unused(argc);
	Str name;
	str_set_cstr(&name, argv[0]);
	login_mgr_delete(&home->login_mgr, &name);
	home_sync(home);
}

static void
main_cmd_bench(Main* self, int argc, char** argv)
{
	// amelie bench name
	auto home = &self->home;
	home_open(home);
	var_int_set(&config()->log_connections, false);

	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);

	Bench bench;
	bench_init(&bench, &remote);
	defer(bench_free, &bench);

	// prepare remote
	error_catch
	(
		login_mgr_set(&self->home.login_mgr, &remote, &bench.vars, argc, argv);
		bench_run(&bench);
	);
}

static void
main_cmd_top(Main* self, int argc, char** argv)
{
	// amelie top name
	auto home = &self->home;
	home_open(home);
	var_int_set(&config()->log_connections, false);

	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);

	Top top;
	top_init(&top, &remote);
	defer(top_free, &top);

	// prepare remote
	error_catch
	(
		login_mgr_set(&self->home.login_mgr, &remote, NULL, argc, argv);
		top_run(&top);
	);
}

static void
main_main(Main* self, int argc, char** argv)
{
	// amelie [command] [options]
	if (argc <= 1)
		goto usage;

	if (!strcmp(argv[1], "-h") ||
	    !strcmp(argv[1], "--help"))
		goto usage;

	if (!strcmp(argv[1], "-v") ||
	    !strcmp(argv[1], "--version"))
	{
		auto version = &state()->version.string;
		info("%.*s", str_size(version), str_of(version));
		return;	
	}

	if (! strcmp(argv[1], "init"))
	{
		// init
		if (argc <= 2)
			goto usage;
		main_cmd_init(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "start"))
	{
		// start
		if (argc <= 2)
			goto usage;
		main_cmd_start(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "stop"))
	{
		// stop
		if (argc <= 2)
			goto usage;
		main_cmd_stop(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "backup"))
	{
		// backup
		if (argc <= 2)
			goto usage;
		main_cmd_backup(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "client"))
	{
		// client
		if (argc <= 2)
			goto usage;
		main_cmd_client(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "import"))
	{
		// import
		if (argc <= 2)
			goto usage;
		main_cmd_import(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "login"))
	{
		// login
		if (argc <= 2)
			goto usage;
		main_cmd_login(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "logout"))
	{
		// logout
		if (argc != 3)
			goto usage;
		main_cmd_logout(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "bench"))
	{
		// benchmark
		if (argc <= 2)
			goto usage;
		main_cmd_bench(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "top"))
	{
		// top
		if (argc <= 2)
			goto usage;
		main_cmd_top(self, argc - 2, argv + 2);
	} else
	{
		// client by default
		main_cmd_client(self, argc - 1, argv + 1);
	}
	return;

usage:
	main_usage();
}

typedef struct
{
	int    argc;
	char** argv;
	Main*  self;
} MainArgs;

static void
main_runner(void* arg)
{
	MainArgs* args = arg;
	Main* self = args->self;

	auto on_error = error_catch
	(
		instance_start(&self->instance);
		main_main(self, args->argc, args->argv);
	);
	instance_stop(&self->instance);

	// complete
	MainRc rc = MAIN_COMPLETE;
	if (on_error)
		rc = MAIN_ERROR;
	cond_signal(&self->task.status, rc);
}

void
main_init(Main* self)
{
	home_init(&self->home);
	instance_init(&self->instance);
	task_init(&self->task);
}

void
main_free(Main* self)
{
	task_free(&self->task);
	home_free(&self->home);
	instance_free(&self->instance);
}

MainRc
main_start(Main* self, int argc, char** argv)
{
	// start cli task
	MainArgs args =
	{
		.argc = argc,
		.argv = argv,
		.self = self
	};
	int rc;
	rc = task_create_nothrow(&self->task, "main", main_runner, &args,
	                         &self->instance.global,
	                         logger_write, &self->instance.logger,
	                         &self->instance.buf_mgr);
	if (unlikely(rc == -1))
		return MAIN_ERROR;

	// wait for cli task to start
	rc = cond_wait(&self->task.status);
	return rc;
}

void
main_stop(Main* self)
{
	auto buf = msg_create_as(&self->instance.buf_mgr, RPC_STOP, 0);
	channel_write(&self->task.channel, buf);
	task_wait(&self->task);
}
