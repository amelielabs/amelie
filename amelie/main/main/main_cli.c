
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
main_execute(Client* client, Str* request)
{
	if (str_empty(request))
		return;

	auto reply = &client->reply.content;
	auto code = client_execute(client, request, NULL);
	switch (code) {
	case 200:
		// 200 OK
		info("{buf}", reply);
		break;
	case 204:
		// 204 No Content
		break;
	default:
	{
		// 400 Bad Request
		// 403 Forbidden
		// 413 Payload Too Large
		if (buf_empty(reply))
		{
			info("error: {d}", code);
			break;
		}
		info("{buf}", reply);
		break;
	}
	}
}

static void
main_console(Main* self, Client* client)
{
	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);

	auto user = opt_string_of(&self->endpoint.user);
	Str prompt_user = *user;
	if (str_empty(&prompt_user))
		str_set(&prompt_user, "amelie", 6);

	// ›
	// ❯
	auto prompt_len = utf8_strlen(&prompt_user) + 2;
	char prompt_str[128];
	format(prompt_str, sizeof(prompt_str), "{str}\033[38;5;147m❯\033[0m ",
	       &prompt_user);

	char prompt_str_pending[128];
	format(prompt_str_pending, sizeof(prompt_str_pending), "{str}  ",
	       &prompt_user);

	Str prompt;
	str_set_cstr(&prompt, prompt_str);

	Str prompt_pending;
	str_set_cstr(&prompt_pending, prompt_str_pending);

	// read and execute commands
	for (;;)
	{
		// >
		auto prompt_ptr = &prompt;
		if (separator_pending(&sep))
			prompt_ptr = &prompt_pending;

		Str input;
		str_init(&input);
		if (! console(&self->console, prompt_ptr, prompt_len, &input))
		{
			if (separator_read_leftover(&sep, &input))
				main_execute(client, &input);
			break;
		}
		defer(str_free, &input);

		// split commands using ; and begin/end stmts
		Str content;
		separator_write(&sep, &input);
		while (separator_read(&sep, &content))
		{
			main_execute(client, &content);
			separator_advance(&sep);
		}
	}
}

void
main_cli(Main* self)
{
	opt_int_set(&config()->log_connections, false);

	// set default content_type
	auto endpoint = &self->endpoint;
	auto content_type = opt_string_of(&endpoint->content_type);
	if (str_empty(content_type))
		opt_string_set_raw(&endpoint->content_type, "text/plain", 10);

	// set default accept
	auto accept = opt_string_of(&endpoint->accept);
	if (str_empty(accept))
		opt_string_set_raw(&endpoint->accept, "text/plain", 10);

	// create client and connect
	auto client = client_allocate();
	defer(client_free, client);
	client_set_endpoint(client, endpoint);
	client_connect(client);

	// read commands
	main_console(self, client);
}
