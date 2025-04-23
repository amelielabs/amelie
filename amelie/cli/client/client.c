
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
#include <amelie_cli.h>
#include <amelie_cli_client.h>

void
cli_cmd_login(Cli* self, int argc, char** argv)
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

void
cli_cmd_logout(Cli* self, int argc, char** argv)
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
cli_cmd_client_execute(Client* client, Str* content)
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
cli_cmd_client_main(Cli* self, Client* client)
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

	auto is_terminal = console_is_terminal();
	for (;;)
	{
		// >
		Str input;
		str_init(&input);
		auto prompt = separator_pending(&sep) ? prompt_ms: prompt_ss;
		if (! console(prompt, &input))
			break;
		defer(str_free, &input);

		// split commands by \n
		if (is_terminal)
		{
			cli_cmd_client_execute(client, &input);
			continue;
		}

		// pipe mode

		// split commands using ; and begin/commit stmts
		Str content;
		separator_write(&sep, &input);
		while (separator_read(&sep, &content))
		{
			cli_cmd_client_execute(client, &content);
			separator_advance(&sep);
		}
	}

	home_sync(&self->home);
}

void
cli_cmd_client(Cli* self, int argc, char** argv)
{
	// amelie client [remote options]
	home_open(&self->home);

	// prepare console and read history
	char path[PATH_MAX];
	home_set_path(path, sizeof(path), "history");
	console_open(path);

	opt_int_set(&config()->log_connections, false);

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
		cli_cmd_client_main(self, client);
	);

	if (client)
	{
		client_close(client);
		client_free(client);
	}

	// sync history
	console_sync(path);
	console_close();
}

void
cli_cmd_import(Cli* self, int argc, char** argv)
{
	// amelie import name
	auto home = &self->home;
	home_open(home);
	opt_int_set(&config()->log_connections, false);

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
		                          &import.opts,
		                           argc,
		                           argv);
		argc -= last;
		argv += last;
		import_run(&import, argc, argv);
	);
}

void
cli_cmd_top(Cli* self, int argc, char** argv)
{
	// amelie top name
	auto home = &self->home;
	home_open(home);
	opt_int_set(&config()->log_connections, false);

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
