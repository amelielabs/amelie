
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>

hot static void
frontend_client_primary_on_write(Primary* self, Buf* data)
{
	auto fe = (Frontend*)((void**)self->replay_arg)[0];
	fe->iface->session_execute_replay(((void**)self->replay_arg)[1], self, data);
}

static void
frontend_client_primary(Frontend* self, Client* client, void* session)
{
	Recover recover;
	recover_init(&recover, share()->db, true);
	defer(recover_free, &recover);

	void* args[] = {self, session};
	Primary primary;
	primary_init(&primary, &recover, client, frontend_client_primary_on_write, args);
	primary_main(&primary);
}

hot static inline bool
frontend_auth(Frontend* self, Client* client)
{
	auto request = &client->request;
	auto auth_header = http_find(request, "Authorization", 13);
	if (! auth_header)
	{
		// trusted by the server listen configuration
		if (! client->auth)
			return false;
	} else
	{
		auto user = auth(&self->auth, &auth_header->value);
		if (likely(user))
			return false;
	}
	// error
	return true;
}

hot static inline bool
frontend_endpoint(Client* client, Output* output)
{
	auto request  = &client->request;
	auto endpoint = client->endpoint;
	endpoint_reset(endpoint);

	// POST /v1/db/<db>
	// POST /v1/db/<db>/<relation>
	// POST /v1/backup
	// POST /v1/repl
	auto method = &request->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		return true;

	// set content-type
	auto content_type = http_find(request, "Content-Type", 12);
	if (likely(content_type))
		endpoint->content_type.string = content_type->value;

	// set accept
	auto accept = http_find(request, "Accept", 6);
	if (likely(accept))
		endpoint->accept.string = accept->value;
	else
		str_set(&endpoint->accept.string, "application/json", 16);

	// parse uri endpoint
	return error_catch
	(
		uri_parse_endpoint(endpoint, &request->options[HTTP_URL]);

		// configure output mime (using Accept) and format
		output_reset(output);
		output_set(output, endpoint);
	);
}

void
frontend_client(Frontend* self, Client* client)
{
	auto readahead = &client->readahead;
	auto request   = &client->request;
	auto reply     = &client->reply.content;

	Endpoint endpoint;
	endpoint_init(&endpoint);
	defer(endpoint_free, &endpoint);
	client_set_endpoint(client, &endpoint);

	Output output;
	output_init(&output);
	output_set_buf(&output, reply);

	// create sesssion
	auto ctl = self->iface;
	auto session = ctl->session_create(self, self->iface_arg);
	defer(ctl->session_free, session);

	for (;;)
	{
		// read request header
		http_reset(request);
		auto eof = http_read(request, readahead, true);
		if (unlikely(eof))
			break;

		// read content
		auto limit = opt_int_of(&config()->limit_recv);
		auto limit_reached =
			http_read_content_limit(request, &client->readahead,
			                        &request->content,
			                        limit);
		if (unlikely(limit_reached))
		{
			// 413 Payload Too Large
			client_413(client);
			break;
		}

		// authenticate
		if (frontend_auth(self, client))
		{
			// 403 Forbidden
			client_403(client);
			continue;
		}

		// parse endpoint and prepare output
		if (frontend_endpoint(client, &output))
		{
			// 400 Bad Request
			client_400(client, NULL);
			continue;
		}

		// handle service requests (backup or primary server connection)
		if (! opt_string_empty(&endpoint.service))
		{
			auto service = &endpoint.service.string;
			if (str_is(service, "backup", 6))
				backup(share()->db, client);
			else
			if (str_is(service, "repl", 4))
				frontend_client_primary(self, client, session);
			break;
		}

		// execute request
		Str content;
		buf_str(&request->content, &content);
		auto on_error = ctl->session_execute(session, &endpoint, &content, &output);
		if (unlikely(on_error))
		{
			// 400 Bad Request
			client_400(client, output.buf);
		} else
		{
			// 204 No Content
			// 200 OK
			if (buf_empty(output.buf))
				client_204(client);
			else
				client_200(client, output.buf);
		}
	}
}
