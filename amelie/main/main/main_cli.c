
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
#include <amelie.h>
#include <amelie_main.h>

static void
main_execute(MainClient* client, Str* request)
{
	if (str_empty(request))
		return;

	Str  reply;
	auto code = main_client_execute(client, request, &reply);
	switch (code) {
	case 200:
		// 200 OK
		info("%.*s", str_size(&reply), str_of(&reply));
		break;
	case 204:
		// 204 No Content
		break;
	default:
	{
		// 400 Bad Request
		// 403 Forbidden
		// 413 Payload Too Large
		if (str_empty(&reply))
		{
			info("error: %d", code);
			break;
		}
		info("error: %.*s", str_size(&reply), str_of(&reply));

#if 0
		// read error message
		Json json;
		json_init(&json);
		defer(json_free, &json);
		json_parse(&json, &reply, NULL);

		// {msg: string}
		auto pos = json.buf->start;
		json_read_obj(&pos);
		// msg
		json_skip(&pos);
		Str text;
		json_read_string(&pos, &text);
		info("error: %.*s", str_size(&text), str_of(&text));
#endif
		break;
	}
	}
}

static void
main_console(Main* self, MainClient* client)
{
	auto name = opt_string_of(&self->endpoint.name);
	auto uri  = opt_string_of(&self->endpoint.uri);
	auto path = opt_string_of(&self->endpoint.path);

	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);

	// set prompt
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

	// read and execute commands
	for (;;)
	{
		// >
		auto prompt_ptr = &prompt;
		if (separator_pending(&sep))
			prompt_ptr = &prompt_pending;

		Str input;
		str_init(&input);
		if (! console(&self->console, prompt_ptr, &input))
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

	// parse command line and open database
	main_open(self, MAIN_OPEN_ANY, NULL);
	defer(main_close, self);

	// set default content_type
	auto endpoint = &self->endpoint;
	auto content_type = opt_string_of(&endpoint->content_type);
	if (str_empty(content_type))
		opt_string_set_raw(&endpoint->content_type, "plain/text", 10);

	// set default accept
	auto accept = opt_string_of(&endpoint->accept);
	if (str_empty(accept))
		opt_string_set_raw(&endpoint->accept, "application/json", 16);

	// create client and connect
	auto client = main_client_create(self);
	defer(main_client_free, client);
	main_client_connect(client);

	// read commands
	main_console(self, client);
}
