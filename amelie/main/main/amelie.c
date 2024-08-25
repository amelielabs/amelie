
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

static void
amelie_usage(void)
{
	auto version = &config()->version.string;
	info("amelie (version: %.*s)", str_size(version), str_of(version));
	info("");
	info("usage: amelie [command | login] [options]");
	info("");
	info("  commands:");
	info("");
	info("    server <path> [server options]");
	info("    backup <path> [login] [remote options]");
	info("    client [login] [remote options]");
	info("    login  <name> [remote options]");
	info("    logout <name>");
	info("");
	info("  remote options:");
	info("");
	int id = 0;
	for (; id < REMOTE_MAX; id++)
		info("    --%s=string", remote_nameof(id));
	info("    --json=string");

	info("");
	info("  server options:");
	info("");
	list_foreach(&config()->list)
	{
		auto var = list_at(Var, link);
		if (!var_is(var, VAR_C) || var_is(var, VAR_Y) ||
		     var_is(var, VAR_H))
			continue;
		char* type;
		switch (var->type) {
		case VAR_BOOL: type = "bool";
			break;
		case VAR_INT: type = "int";
			break;
		case VAR_STRING: type = "string";
			break;
		case VAR_DATA: type = "json";
			break;
		}
		info("    --%.*s=%s", str_size(&var->name),
		     str_of(&var->name), type);
	}
	info("    --json=string");
	info("");
}

static void
amelie_server(Amelie* self, int argc, char** argv)
{
	// amelie server path [server options]
	auto bootstrap = main_open(&self->main, argv[0], false);

	System* system = NULL;
	Exception e;
	if (enter(&e))
	{
		system = system_create();
		system_start(system, bootstrap, argc - 1, argv + 1);

		// notify amelie_start about start completion
		status_set(&self->task.status, AMELIE_RUN);

		// handle system requests
		system_main(system);
	}

	if (leave(&e))
	{ }

	// shutdown
	if (system)
	{
		system_stop(system);
		system_free(system);
	}
}

static void
amelie_backup(Amelie* self, int argc, char** argv)
{
	home_open(&self->home);

	// amelie backup path [remote options]
	auto bootstrap = main_open(&self->main, argv[0], true);
	if (! bootstrap)
		error("directory already exists");

	// prepare remote
	Remote remote;
	remote_init(&remote);
	guard(remote_free, &remote);
	login_mgr_set(&self->home.login_mgr, &remote, argc - 1, argv + 1);

	// disable log output
	if (str_compare_cstr(remote_get(&remote, REMOTE_DEBUG), "0"))
		logger_set_to_stdout(&self->main.logger, false);

	restore(&remote);
}

static void
amelie_client_main(Client* client)
{
	auto request = &client->request;
	auto reply   = &client->reply;
	auto token   = remote_get(client->remote, REMOTE_TOKEN);
	for (;;)
	{
		// >
		if (isatty(fileno(stdin)))
		{
			printf("> ");
			fflush(stdout);
		}

		// read command
		char text[1024];
		if (! fgets(text, sizeof(text), stdin))
			break;
		Str content;
		str_set_cstr(&content, text);
		if (! str_size(&content))
			continue;

		// request
		http_write_request(request, "POST /");
		if (! str_empty(token))
			http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
		http_write(request, "Content-Length", "%d", str_size(&content));
		http_write(request, "Content-Type", "text/plain");
		http_write_end(request);
		tcp_write_pair_str(&client->tcp, &request->raw, &content);

		// reply
		http_reset(reply);
		auto eof = http_read(reply, &client->readahead, false);
		if (eof)
			error("unexpected eof");
		http_read_content(reply, &client->readahead, &reply->content);

		// print
		printf("%.*s\n", buf_size(&reply->content), reply->content.start);
	}
}

static void
amelie_client(Amelie* self, int argc, char** argv)
{
	// amelie client [remote options]
	home_open(&self->home);

	Remote remote;
	remote_init(&remote);

	Client* client = NULL;

	Exception e;
	if (enter(&e))
	{
		// prepare remote
		login_mgr_set(&self->home.login_mgr, &remote, argc, argv);

		// create client and connect
		client = client_create();
		client_set_remote(client, &remote);
		client_connect(client);

		// process cli
		amelie_client_main(client);
	}

	if (leave(&e))
	{ }

	if (client)
	{
		client_close(client);
		client_free(client);
	}

	remote_free(&remote);
	status_set(&self->task.status, AMELIE_COMPLETE);
}

static void
amelie_login(Amelie* self, int argc, char** argv)
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
		guard(login_free, login);
		remote_set(&login->remote, REMOTE_NAME, &name);
		login_mgr_add(&home->login_mgr, login);
		unguard();
	}

	login_mgr_set(&home->login_mgr, &login->remote, argc - 1, argv + 1);
	home_sync(home);
}

static void
amelie_logout(Amelie* self, int argc, char** argv)
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
amelie_main(Amelie* self, int argc, char** argv)
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
		auto version = &config()->version.string;
		info("%.*s", str_size(version), str_of(version));
		return;	
	}

	if (! strcmp(argv[1], "server"))
	{
		// server
		if (argc <= 2)
			goto usage;
		amelie_server(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "backup"))
	{
		// backup
		if (argc <= 2)
			goto usage;
		amelie_backup(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "client"))
	{
		// client
		if (argc <= 2)
			goto usage;
		amelie_client(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "login"))
	{
		// login
		if (argc <= 2)
			goto usage;
		amelie_login(self, argc - 2, argv + 2);
	} else
	if (! strcmp(argv[1], "logout"))
	{
		// logout
		if (argc != 3)
			goto usage;
		amelie_logout(self, argc - 2, argv + 2);
	} else
	{
		// client by default
		amelie_client(self, argc - 1, argv + 1);
	}
	return;

usage:
	amelie_usage();
}

typedef struct
{
	int    argc;
	char** argv;
	Amelie*   self;
} AmelieArgs;

static void
amelie_runner(void* arg)
{
	AmelieArgs* args = arg;
	Amelie* self = args->self;

	Exception e;
	if (enter(&e))
	{
		main_start(&self->main);
		amelie_main(self, args->argc, args->argv);
	}
	if (leave(&e))
	{ }

	main_stop(&self->main);

	// complete
	AmelieRc rc = AMELIE_COMPLETE;
	if (e.triggered)
		rc = AMELIE_ERROR;
	status_set(&self->task.status, rc);
}

void
amelie_init(Amelie* self)
{
	home_init(&self->home);
	main_init(&self->main);
	task_init(&self->task);
}

void
amelie_free(Amelie* self)
{
	home_free(&self->home);
	main_free(&self->main);
	task_free(&self->task);
}

AmelieRc
amelie_start(Amelie* self, int argc, char** argv)
{
	// start cli task
	AmelieArgs args =
	{
		.argc = argc,
		.argv = argv,
		.self = self
	};
	int rc;
	rc = task_create_nothrow(&self->task, "main", amelie_runner, &args,
	                         &self->main.global,
	                         logger_write, &self->main.logger);
	if (unlikely(rc == -1))
		return AMELIE_ERROR;

	// wait for cli task to start
	rc = status_wait(&self->task.status);
	return rc;
}

void
amelie_stop(Amelie* self)
{
	auto buf = msg_create_nothrow(&self->task.buf_cache, RPC_STOP, 0);
	if (! buf)
		abort();
	channel_write(&self->task.channel, buf);
	task_wait(&self->task);
}
