
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
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
	recover_init(&recover, share()->storage, true);
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
	auto method = &request->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		goto forbidden;

	auto auth_header = http_find(request, "Authorization", 13);
	if (! auth_header)
	{
		// trusted by the server listen configuration
		if (! client->auth)
			return true;
	} else
	{
		auto user = auth(&self->auth, &auth_header->value);
		if (likely(user))
			return true;
	}

forbidden:
	// 403 Forbidden
	client_403(client);
	return false;
}

hot static inline void
frontend_endpoint(Client* client)
{
	auto request  = &client->request;
	auto endpoint = client->endpoint;
	endpoint_reset(endpoint);

	// POST /v1/db/<db_name>/tables/<name>
	// POST /v1/db/<db_name>/functions/<name>
	// POST /v1/db/<db_name>
	// POST /v1/backup
	// POST /v1/repl

	// parse uri endpoint
	uri_parse_endpoint(endpoint, &request->options[HTTP_URL]);

	// set content-type
	auto content_type = http_find(request, "Content-Type", 12);
	if (content_type)
		endpoint->content_type.string = content_type->value;

	// set accept
	auto accept = http_find(request, "Accept", 6);
	if (accept)
		endpoint->accept.string = accept->value;

	// find and parse Prefer header
	auto prefer = http_find(request, "Prefer", 6);
	if (prefer)
		prefer_parse(endpoint, &prefer->value);
}

void
frontend_client(Frontend* self, Client* client)
{
	auto readahead = &client->readahead;
	auto request   = &client->request;

	Endpoint endpoint;
	endpoint_init(&endpoint);
	defer(endpoint_free, &endpoint);
	client_set_endpoint(client, &endpoint);

	Content output;
	content_init(&output);
	content_set(&output, &client->reply.content);

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

		// authenticate
		if (! frontend_auth(self, client))
			break;

		// parse endpoint
		frontend_endpoint(client);

		// handle backup or primary server connection
		if (! opt_string_empty(&endpoint.service))
		{
			auto service = &endpoint.service.string;
			if (str_is(service, "backup", 6))
				backup(share()->storage, client);
			else
			if (str_is(service, "repl", 4))
				frontend_client_primary(self, client, session);
			break;
		}

		// ensure there are a content type and access headers
		if (opt_string_empty(&endpoint.content_type) ||
		    opt_string_empty(&endpoint.accept))
		{
			// 403 Forbidden
			client_403(client);
			continue;
		}

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
			continue;
		}

		Str content;
		buf_str(&request->content, &content);

		// execute request
		content_reset(&output);
		auto on_error = ctl->session_execute(session, &endpoint, &content, &output);
		if (unlikely(on_error))
		{
			// 400 Bad Request
			client_400(client, output.content);
		} else
		{
			// 204 No Content
			// 200 OK
			if (buf_empty(output.content))
				client_204(client);
			else
				client_200(client, output.content);
		}
	}
}
