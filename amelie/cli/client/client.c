
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

#include <amelie_core.h>
#include <amelie_cli.h>
#include <amelie_cli_client.h>

void
cli_cmd_login(int argc, char** argv)
{
	// amelie login name [remote options]
	Home home;
	home_init(&home);
	defer(home_free, &home);
	home_open(&home);

	Str name;
	str_set_cstr(&name, argv[0]);
	auto login = login_mgr_find(&home.login_mgr, &name);
	if (! login)
	{
		login = login_allocate();
		remote_set(&login->remote, REMOTE_NAME, &name);
		login_mgr_add(&home.login_mgr, login);
	}

	login_mgr_set(&home.login_mgr, &login->remote, NULL, argc - 1, argv + 1);
	home_sync(&home);
}

void
cli_cmd_logout(int argc, char** argv)
{
	unused(argc);

	// amelie logout name
	Home home;
	home_init(&home);
	defer(home_free, &home);
	home_open(&home);

	Str name;
	str_set_cstr(&name, argv[0]);
	login_mgr_delete(&home.login_mgr, &name);
	home_sync(&home);
}

static void
cli_cmd_client_print_error(Client* client)
{
	auto reply = &client->reply;
	auto code  = &reply->options[HTTP_CODE];
	auto msg   = &reply->options[HTTP_MSG];

	Str content;
	buf_str(&reply->content, &content);
	if (str_empty(&content))
	{
		printf("error: %.*s %.*s\n", str_size(code), str_of(code),
		       str_size(msg), str_of(msg));
		return;
	}

	// read error message
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &content, NULL);

	// {msg: string}
	auto pos = json.buf->start;
	json_read_obj(&pos);
	// msg
	json_skip(&pos);
	Str text;
	json_read_string(&pos, &text);
	printf("error: %.*s\n", str_size(&text), str_of(&text));
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

	// 400 Bad Request
	// 403 Forbidden
	// 413 Payload Too Large
	if (str_is(&reply->options[HTTP_CODE], "400", 3) ||
	    str_is(&reply->options[HTTP_CODE], "403", 3) ||
	    str_is(&reply->options[HTTP_CODE], "413", 3))
	{
		cli_cmd_client_print_error(client);
		return;
	}

	// 204 No Content
	if (str_is(&reply->options[HTTP_CODE], "204", 3))
		return;

	// 200 OK
	printf("%.*s\n", buf_size(&reply->content),
	       reply->content.start);
}

static void
cli_cmd_client_main(Client* client, Console* cons)
{
	auto name = remote_get(client->remote, REMOTE_NAME);
	auto uri  = remote_get(client->remote, REMOTE_URI);
	auto path = remote_get(client->remote, REMOTE_PATH);

	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);

	Str* prompt_text;
	if (! str_empty(name))
		prompt_text = name;
	else
	if (! str_empty(path))
		prompt_text = path;
	else
		prompt_text = uri;

	char prompt_str[128];
	snprintf(prompt_str, sizeof(prompt_str), "%.*s> ", str_size(prompt_text),
	         str_of(prompt_text));
	Str prompt;
	str_set_cstr(&prompt, prompt_str);

	char prompt_str_pending[128];
	snprintf(prompt_str_pending, sizeof(prompt_str_pending), "%.*s- ",
	         str_size(prompt_text), str_of(prompt_text));
	Str prompt_pending;
	str_set_cstr(&prompt_pending, prompt_str_pending);

	for (;;)
	{
		// >
		auto prompt_ptr = &prompt;
		if (separator_pending(&sep))
			prompt_ptr = &prompt_pending;

		Str input;
		str_init(&input);
		if (! console(cons, prompt_ptr, &input))
		{
			if (separator_read_leftover(&sep, &input))
				cli_cmd_client_execute(client, &input);
			break;
		}
		defer(str_free, &input);

		// split commands using ; and begin/end stmts
		Str content;
		separator_write(&sep, &input);
		while (separator_read(&sep, &content))
		{
			cli_cmd_client_execute(client, &content);
			separator_advance(&sep);
		}
	}
}

void
cli_cmd_client(int argc, char** argv)
{
	// amelie client [remote options]
	Home home;
	home_init(&home);
	defer(home_free, &home);
	home_open(&home);

	// prepare console and read history
	Console console;
	console_init(&console);
	char path[PATH_MAX];
	home_set_path(path, sizeof(path), "history");
	console_load(&console, path);

	opt_int_set(&config()->log_connections, false);

	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);

	Client* client = NULL;
	error_catch
	(
		// prepare remote
		login_mgr_set(&home.login_mgr, &remote, NULL, argc, argv);

		// create client and connect
		client = client_create();
		client_set_remote(client, &remote);
		client_connect(client);

		// process cli
		cli_cmd_client_main(client, &console);
	);

	if (client)
	{
		client_close(client);
		client_free(client);
	}
	home_sync(&home);

	// sync history
	console_save(&console, path);
	console_free(&console);
}

void
cli_cmd_import(int argc, char** argv)
{
	// amelie import name
	Home home;
	home_init(&home);
	defer(home_free, &home);
	home_open(&home);

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
		auto last = login_mgr_set(&home.login_mgr, &remote,
		                          &import.opts,
		                           argc,
		                           argv);
		argc -= last;
		argv += last;
		import_run(&import, argc, argv);
	);
}

void
cli_cmd_top(int argc, char** argv)
{
	// amelie top name
	Home home;
	home_init(&home);
	defer(home_free, &home);
	home_open(&home);

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
		login_mgr_set(&home.login_mgr, &remote, NULL, argc, argv);
		top_run(&top);
	);
}
